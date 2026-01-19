#!/usr/bin/env python3
import sys
import os
import json
from pathlib import Path

import psycopg

from model_client import ModelConfig, get_model_client
from prompt_templates.query_expander_template import QUERY_EXPAND_TEMPLATE
from config import settings

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

DSN: str = "postgresql://leonardjin@127.0.0.1:5432/csha_dev"
# Dummy model name so vector paths effectively return nothing
DUMMY_MODEL_NAME = "NO_VECTOR_TEST_MODEL"
VECTOR_DIM = 3072

# File names (in same directory as this script)
SECTIONS_JSONL = "sections.jsonl"
DOCS_JSONL = "website_data_documents.jsonl"
SECTION_CHUNKS_JSONL = "section_chunks.jsonl"
DOC_CHUNKS_JSONL = "website_data_document_chunks.jsonl"


# -------------------------------------------------------------------
# UTIL: load SQL
# -------------------------------------------------------------------

def load_sql_files(base_dir: Path):
    stage1_path = base_dir / "stage1_document_section_filter.sql"
    stage2_path = base_dir / "stage2_chunk_retrieval.sql"

    with stage1_path.open("r", encoding="utf-8") as f:
        stage1_sql = f.read()

    with stage2_path.open("r", encoding="utf-8") as f:
        stage2_sql = f.read()

    return stage1_sql, stage2_sql


# -------------------------------------------------------------------
# UTIL: index JSONL by UUID/ID but keep raw lines
# -------------------------------------------------------------------

def load_jsonl_index(path, key):
    """
    Build a dict: key_value -> raw_line (stripped '\n').

    `key` is something like "section_uuid", "document_uuid", "chunk_uuid".
    """
    idx = {}
    path = Path(path)
    if not path.exists():
        print(f"WARNING: JSONL file not found: {path}")
        return idx

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.rstrip("\n")
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                print(f"WARNING: bad JSON line in {path}: {raw[:80]}...")
                continue
            val = obj.get(key)
            if val is None:
                continue
            idx[str(val)] = raw

    # print(f"Indexed {len(idx)} items from {path} by '{key}'")
    return idx


# -------------------------------------------------------------------
# STAGE 1
# -------------------------------------------------------------------

def run_stage1(conn, stage1_sql, query_text):
    """
    Run stage 1 and return:
      - rows: list[dict]
      - doc_uuids: list[str]
      - sec_uuids: list[str]
    """
    zero_vec = [0.0] * VECTOR_DIM
    print("zero_vec: ", zero_vec)

    params = {
        "query": query_text,
        "vector": zero_vec,
        "model_name": DUMMY_MODEL_NAME,
        "include_docs": True,
        "include_sections": True,
    }

    with conn.cursor() as cur:
        cur.execute(stage1_sql, params)
        colnames = [desc[0] for desc in cur.description]
        rows = [dict(zip(colnames, row)) for row in cur.fetchall()]

    doc_uuids = []
    sec_uuids = []

    for r in rows:
        stype = r["source_type"]
        uid = str(r["source_uuid"])
        if stype == "document":
            doc_uuids.append(uid)
        elif stype == "section":
            sec_uuids.append(uid)

    return rows, doc_uuids, sec_uuids


def write_stage1_jsonl(doc_uuids, sec_uuids, docs_index, secs_index, output_path):
    """
    Stage 1 output:
    - only the raw JSONL objects from docs/sections that match returned UUIDs.
    - Dedup by UUID while preserving order.
    """
    out_path = Path(output_path)
    seen = set()

    with out_path.open("w", encoding="utf-8") as f:
        # Documents first, preserving stage1 order
        for uid in doc_uuids:
            if uid in seen:
                continue
            seen.add(uid)
            raw = docs_index.get(uid)
            if raw is None:
                print(f"WARNING: doc UUID {uid} not found in {DOCS_JSONL}")
                continue
            f.write(raw + "\n")

        # Then sections
        for uid in sec_uuids:
            if uid in seen:
                continue
            seen.add(uid)
            raw = secs_index.get(uid)
            if raw is None:
                print(f"WARNING: section UUID {uid} not found in {SECTIONS_JSONL}")
                continue
            f.write(raw + "\n")

    # print(f"Stage 1 JSONL written to: {out_path}")


# -------------------------------------------------------------------
# STAGE 2
# -------------------------------------------------------------------

def run_stage2(conn, stage2_sql_template, query_text, doc_uuids, sec_uuids, top_k=40):
    """
    Run stage 2, using same "disable vectors" trick.

    NOTE: stage2 SQL in file has 'LIMIT %TOP_K%'. We replace
    that placeholder with the integer, since it's not a bound param.
    """
    zero_vec = [0.0] * VECTOR_DIM

    params = {
        "query": query_text,
        "vector": zero_vec,
        "model_name": DUMMY_MODEL_NAME,
        "document_uuids": doc_uuids,
        "section_uuids": sec_uuids,
        "vector_weight": 0.7,
        "keyword_weight": 0.3,
    }

    stage2_sql = stage2_sql_template.replace("%TOP_K%", str(top_k))

    with conn.cursor() as cur:
        cur.execute(stage2_sql, params)
        colnames = [desc[0] for desc in cur.description]
        rows = [dict(zip(colnames, row)) for row in cur.fetchall()]

    return rows


def write_stage2_jsonl(stage2_rows, doc_chunks_index, sec_chunks_index, output_path):
    """
    Stage 2 output:
    - only the raw JSONL objects for the chunk_uuids returned by stage 2.
    - Uses source_type to decide which index to use.
    - Dedup by chunk_uuid while preserving stage2 rank order.
    """
    out_path = Path(output_path)
    seen_chunks = set()

    with out_path.open("w", encoding="utf-8") as f:
        for row in stage2_rows:
            chunk_uuid = row.get("chunk_uuid")
            if chunk_uuid is None:
                continue
            chunk_uuid = str(chunk_uuid)
            if chunk_uuid in seen_chunks:
                continue
            seen_chunks.add(chunk_uuid)

            source_type = row.get("source_type")
            if source_type == "document":
                raw = doc_chunks_index.get(chunk_uuid)
                src_label = DOC_CHUNKS_JSONL
            else:
                raw = sec_chunks_index.get(chunk_uuid)
                src_label = SECTION_CHUNKS_JSONL

            if raw is None:
                print(
                    f"WARNING: chunk UUID {chunk_uuid} (type={source_type}) "
                    f"not found in {src_label}"
                )
                continue

            f.write(raw + "\n")

    print(f"Stage 2 JSONL written to: {out_path}")


def expand_query(query_text: str) -> str:
    model_config = ModelConfig(
            model_type="query",
            model_name="gpt-4.1-mini",
            temperature=0.0
        )
    model_client = get_model_client(model_config)

    prompt = QUERY_EXPAND_TEMPLATE.format(query=query_text)
    enhanced_query = model_client.invoke(prompt).content.strip()
    
    print(f"\n Query Expansion: '{query_text}' → '{enhanced_query}'")
    
    return enhanced_query

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: python test_two_stage_keyword_only.py 'your query here'")
        sys.exit(1)

    query_text = sys.argv[1]
    print(f"Running two-stage keyword-only test for query: {query_text}")

    # query_text = "@*(" + expand_query(query_text) + ")"
    query_text = expand_query(query_text)
    # Load paths
    base_dir = Path(__file__).resolve().parent
    input_data_dir = base_dir / "input_data"
    output_data_dir = base_dir / "output_data"

    sections_index = load_jsonl_index(input_data_dir / SECTIONS_JSONL, "section_uuid")
    docs_index = load_jsonl_index(input_data_dir / DOCS_JSONL, "document_uuid")

    # Load chunk JSONL indices (by chunk_uuid) for stage 2
    doc_chunks_index = load_jsonl_index(input_data_dir / DOC_CHUNKS_JSONL, "chunk_uuid")
    sec_chunks_index = load_jsonl_index(input_data_dir / SECTION_CHUNKS_JSONL, "chunk_uuid")

    stage1_sql, stage2_sql = load_sql_files(base_dir)

    with psycopg.connect(DSN) as conn:
        # --------------------------
        # Stage 1
        # --------------------------
        stage1_rows, doc_uuids, sec_uuids = run_stage1(conn, stage1_sql, query_text)
        print(f"\n Stage 1 returned {len(stage1_rows)} units")
        print(f"  document UUIDs: {len(doc_uuids)}")
        print(f"  section UUIDs : {len(sec_uuids)} \n")

        write_stage1_jsonl(
            doc_uuids,
            sec_uuids,
            docs_index,
            sections_index,
            output_data_dir / "stage1_output.jsonl",
        )

        # --------------------------
        # Stage 2
        # --------------------------

        stage2_rows = run_stage2(
            conn,
            stage2_sql,
            query_text,
            doc_uuids,
            sec_uuids,
            top_k=20,
        )
        print(f"Stage 2 returned {len(stage2_rows)} chunk rows")

        write_stage2_jsonl(
            stage2_rows,
            doc_chunks_index,
            sec_chunks_index,
            output_data_dir / "stage2_output.jsonl",
        )


if __name__ == "__main__":
    main()
