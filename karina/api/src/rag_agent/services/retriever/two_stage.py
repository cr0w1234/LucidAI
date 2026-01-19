import psycopg
import psycopg_pool
import logging
import time
from typing import List, Dict, Any, Tuple
from rag_agent.core.model_client import get_model_client, ModelConfig, ModelType
from rag_agent.core.config import settings
from rag_agent.services.retriever.base_retriever import BaseRetriever
from rag_agent.services.ner_extractor import NERKeywordExtractor
from rag_agent.services.query_expander import QueryExpander

logger = logging.getLogger(__name__)


class TwoStageRetriever(BaseRetriever):
    """
    Two-stage retrieval system:
      - Stage 1: recall-heavy unit filter (documents + sections) using rank fusion (no weights).
      - Stage 2: chunk retrieval with authoritative 70/30 fusion (semantic/keyword).

    Notes:
      - Stage-1 returns a mixed set of document + section UUIDs (deduped by identity).
      - Stage-2 fuses semantic/keyword at the chunk level only (0.7/0.3).
    """

    def __init__(
        self,
        dsn: str,
        top_k: int = 16,
        sql_timeout_s: float = 10.0,
        vector_weight: float = 0.7,
        keyword_weight: float = 0.3,
    ):
        self.dsn = dsn
        # Keep original arg names for compatibility; Stage-1 will internally cap to 75.
        self.top_k = top_k
        self.sql_timeout_s = sql_timeout_s
        self.vector_weight = vector_weight
        self.keyword_weight = keyword_weight

        # Create connection pool for better performance
        self.pool = psycopg_pool.ConnectionPool(
            dsn,
            min_size=1,
            max_size=3,
            timeout=sql_timeout_s
        )

        # Initialize models
        self.embedding_model = settings.EMBEDDING_MODEL
        self.embedding_config = ModelConfig(
            model_type=ModelType.EMBEDDING,
            model_name=self.embedding_model
        )

        # Initialize components
        self.ner_extractor = NERKeywordExtractor()
        self.query_expander = QueryExpander()
        self.embedding_client = get_model_client(self.embedding_config)

    def _embed_query(self, query: str) -> List[float]:
        """Generate embedding for a query."""
        return self.embedding_client.embed_query(query)

    def _log_results_from_data(
        self,
        all_units: List[Dict[str, Any]],
    ) -> None:
        def _safe_title(u: Dict[str, Any], n: int = 60) -> str:
            t = u.get("title") or ""
            return t[:n]

        def _fmt_score(v: Any) -> str:
            return f"{float(v):.4f}" if v is not None else "None"

        def _sources(u: Dict[str, Any]) -> List[str]:
            # retrieval_sources is typically a list from SQL array_agg; be defensive.
            rs = u.get("retrieval_sources")
            if rs is None:
                return []
            if isinstance(rs, list):
                return rs
            return [str(rs)]

        def _has_kw(u: Dict[str, Any]) -> bool:
            # Prefer stream membership if present; fallback to kw_score non-NULL.
            rs = _sources(u)
            if rs:
                return any("kw" in s for s in rs)
            return u.get("best_kw_score") is not None

        def _has_sem(u: Dict[str, Any]) -> bool:
            rs = _sources(u)
            if rs:
                return any("vec" in s for s in rs)
            return u.get("best_sem_score") is not None

        def _mode(u: Dict[str, Any]) -> str:
            kw = _has_kw(u)
            sem = _has_sem(u)
            if kw and sem:
                return "hybrid"
            if kw:
                return "keyword"
            if sem:
                return "semantic"
            return "none"

        # --- Partition units by mode (keyword / semantic / hybrid) and type (document/section)
        docs = [u for u in all_units if u.get("source_type") == "document"]
        secs = [u for u in all_units if u.get("source_type") == "section"]

        # Summary counts
        def _count_mode(units: List[Dict[str, Any]], m: str) -> int:
            return sum(1 for u in units if _mode(u) == m)

        logger.info(
            "\nSTAGE 1 RETRIEVAL BREAKDOWN: "
            f"docs={len(docs)} (kw={_count_mode(docs,'keyword')}, sem={_count_mode(docs,'semantic')}, hybrid={_count_mode(docs,'hybrid')}), "
            f"secs={len(secs)} (kw={_count_mode(secs,'keyword')}, sem={_count_mode(secs,'semantic')}, hybrid={_count_mode(secs,'hybrid')})"
        )

        # --- Keyword list: include anything that has keyword signal (keyword-only or hybrid)
        kw_units = [u for u in all_units if _has_kw(u)]
        # If rank_kw exists, use it; otherwise sort by kw_score desc (None last)
        kw_units_sorted = sorted(
            kw_units,
            key=lambda u: (
                u.get("rank_kw") if u.get("rank_kw") is not None else 2147483647,
                -(float(u["best_kw_score"]) if u.get("best_kw_score") is not None else float("-inf")),
            ),
        )

        kw_docs = [u for u in kw_units_sorted if u.get("source_type") == "document"]
        kw_secs = [u for u in kw_units_sorted if u.get("source_type") == "section"]

        logger.info(f"\nKEYWORD RETRIEVAL (Stage 1): {len(kw_docs)} documents, {len(kw_secs)} sections")
        if kw_docs:
            logger.info(f"Keyword Documents (top {min(30, len(kw_docs))}):")
            for i, doc in enumerate(kw_docs[:30], 1):
                logger.info(
                    f"  {i}. {_safe_title(doc)} "
                    f"(mode={_mode(doc)}, kw_score={_fmt_score(doc.get('best_kw_score'))}, sem_score={_fmt_score(doc.get('best_sem_score'))}, sources={_sources(doc)})"
                )
        if kw_secs:
            logger.info(f"Keyword Sections (top {min(30, len(kw_secs))}):")
            for i, sec in enumerate(kw_secs[:30], 1):
                logger.info(
                    f"  {i}. {_safe_title(sec)} "
                    f"(mode={_mode(sec)}, kw_score={_fmt_score(sec.get('best_kw_score'))}, sem_score={_fmt_score(sec.get('best_sem_score'))}, sources={_sources(sec)})"
                )
        if not kw_docs and not kw_secs:
            logger.warning("  No keyword results found!")

        # --- Semantic list: include anything that has semantic signal (semantic-only or hybrid)
        sem_units = [u for u in all_units if _has_sem(u)]
        # Sort by sem_score desc (None last). Negative sem_scores are still valid.
        sem_units_sorted = sorted(
            sem_units,
            key=lambda u: (float(u["best_sem_score"]) if u.get("best_sem_score") is not None else float("-inf")),
            reverse=True,
        )

        sem_docs = [u for u in sem_units_sorted if u.get("source_type") == "document"]
        sem_secs = [u for u in sem_units_sorted if u.get("source_type") == "section"]

        logger.info(f"\nSEMANTIC RETRIEVAL (Stage 1): {len(sem_docs)} documents, {len(sem_secs)} sections")
        if sem_docs:
            logger.info(f"Semantic Documents (top {min(30, len(sem_docs))}):")
            for i, doc in enumerate(sem_docs[:30], 1):
                logger.info(
                    f"  {i}. {_safe_title(doc)} "
                    f"(mode={_mode(doc)}, sem_score={_fmt_score(doc.get('best_sem_score'))}, kw_score={_fmt_score(doc.get('best_kw_score'))}, sources={_sources(doc)})"
                )
        if sem_secs:
            logger.info(f"Semantic Sections (top {min(30, len(sem_secs))}):")
            for i, sec in enumerate(sem_secs[:30], 1):
                logger.info(
                    f"  {i}. {_safe_title(sec)} "
                    f"(mode={_mode(sec)}, sem_score={_fmt_score(sec.get('best_sem_score'))}, kw_score={_fmt_score(sec.get('best_kw_score'))}, sources={_sources(sec)})"
                )
        if not sem_docs and not sem_secs:
            logger.warning("  No semantic results found!")



    # -----------------------------
    # New Stage-1 (rank fusion) API
    # -----------------------------
    def _stage1_unit_filter(
        self,
        query: str,
        query_vector: List[float],
        include_docs: bool = True,
        include_sections: bool = True,
        cap_units: int = 75,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Stage 1: Recall-heavy unit filter with rank fusion (no weights).
        Returns two lists:
        - documents: [{uuid, title, link, best_sem_score, best_kw_score, retrieval_sources}, ...]
        - sections : [same keys, ...]
        """
        try:
            stage1_sql_path = settings.SQL_DIR / "stage1_document_section_filter.sql"
            stage1_sql = open(stage1_sql_path).read()

            documents: List[Dict[str, Any]] = []
            sections: List[Dict[str, Any]] = []
            all_units: List[Dict[str, Any]] = []

            # IMPORTANT: rows must be defined outside the cursor scope so we can use it later.
            rows = []

            with self.pool.connection() as db_connection:
                with db_connection.cursor() as db_cursor:
                    # Set timeout
                    db_cursor.execute(
                        f"SET LOCAL statement_timeout = {int(self.sql_timeout_s * 1000)};"
                    )

                    # Execute Stage 1 query
                    db_cursor.execute(
                        stage1_sql,
                        {
                            "query_text": query,
                            "vector": query_vector,
                            "model_name": self.embedding_model,
                            "include_docs": include_docs,
                            "include_sections": include_sections,
                        },
                    )

                    # debug: ensure we actually have a result set
                    if db_cursor.description is None:
                        logger.error("STAGE1: NO RESULT SET (db_cursor.description is None)")
                        logger.error("STAGE1 SQL HEAD:\n%s", stage1_sql[:300])
                        logger.error("STAGE1 SQL TAIL:\n%s", stage1_sql[-800:])
                        return [], []

                    # Fetch rows while cursor is alive
                    rows = db_cursor.fetchall()

                    # debug: column order (ground truth)
                    logger.error("STAGE1 COLUMN ORDER:")
                    for i, col in enumerate(db_cursor.description):
                        logger.error("  r[%d] = %s (%s)", i, col.name, col.type_code)

                    # debug: first row raw values
                    if rows:
                        logger.error("STAGE1 FIRST ROW RAW:")
                        for i, val in enumerate(rows[0]):
                            logger.error("  r[%d] = %r", i, val)
                    else:
                        logger.error("STAGE1 RETURNED NO ROWS")

            # Normal processing after cursor closes (rows are already fetched)
            for r in rows[:cap_units]:
                source_type = r[0]
                unit = {
                    "source_type": source_type,
                    "uuid": r[1],
                    "title": r[2],
                    "link": r[3],
                    "best_sem_score": float(r[4]) if r[4] is not None else None,
                    "best_kw_score": float(r[5]) if r[5] is not None else None,
                    "retrieval_sources": r[6],
                }
                all_units.append(unit)
                if source_type == "document":
                    documents.append(unit)
                else:
                    sections.append(unit)

            # Log breakdown using the fetched units
            self._log_results_from_data(all_units)

            logger.info(
                f"\n====================================================== Stage 1: {len(documents)} documents, {len(sections)} sections (total {len(rows)}) ======================================================"
            )

            return documents, sections

        except Exception as e:
            logger.exception(f"Error in Stage 1 (rank fusion) filtering: {e}")
            return [], []


    # --------------------------------------------
    # Back-compat Stage-1 method (kept, minimal)
    # --------------------------------------------
    def _stage1_document_filter(
        self, 
        query_text: str, 
        query_vector: List[float]
    ) -> List[Dict[str, Any]]:
        """
        Backward-compatible wrapper that returns only documents in the old shape.
        NOTE: Uses the new rank-fusion Stage-1 internally.
        """
        docs, _secs = self._stage1_unit_filter(query_text, query_vector)
        documents = []
        for doc in docs:
            documents.append({
                "document_uuid": doc["uuid"],
                "title": doc["title"],
                "excerpt": None,  # not provided by rank-fusion Stage-1 (by design)
                # Use best_sem_score as a placeholder for compatibility logging
                "combined_score": float(doc.get("best_sem_score", 0.0))
            })
        logger.info(f"\n====================================================== Stage 1: Returned {len(documents)} documents ======================================================")
        return documents

    # -----------------------------
    # New Stage-2 (70/30 fusion) API
    # -----------------------------
    def _stage2_chunk_retrieval_fusion(
        self,
        query: str,
        query_vector: List[float],
        document_uuids: List[str],
        section_uuids: List[str],
    ) -> List[str]:
        """
        Stage 2: Retrieve chunks with authoritative 70/30 fusion (semantic/keyword).
        Returns formatted chunk blocks:
          <text>...</text>\n<reference><url>...</url></reference>
        """
        try:
            stage2_sql = (
                open(settings.SQL_DIR / "stage2_chunk_retrieval.sql").read()
                .replace("%TOP_K%", str(self.top_k))
            )

            with self.pool.connection() as db_connection:
                with db_connection.cursor() as db_cursor:
                    # Set timeout
                    db_cursor.execute(f"SET LOCAL statement_timeout = {int(self.sql_timeout_s * 1000)};")

                    # Execute Stage 2 (fusion) query
                    db_cursor.execute(
                        stage2_sql,
                        {
                            "query_text": query,
                            "vector": query_vector,
                            "model_name": self.embedding_model,
                            "document_uuids": document_uuids,
                            "section_uuids": section_uuids,
                            "vector_weight": self.vector_weight,
                            "keyword_weight": self.keyword_weight,
                        },
                    )

                    if db_cursor.description is None:
                        logger.error("STAGE2: NO RESULT SET. SQL TAIL:\n%s", stage2_sql[-800:])
                        return []

                    rows = db_cursor.fetchall() 

                    # rows: (source_type, source_uuid, content_chunk, link, combined_score)
                    chunks_by_url: Dict[str, str] = {}
                    for row in rows:
                        content_chunk = row[2]
                        link = row[3] if len(row) > 3 and row[3] else ""
                        if link in chunks_by_url:
                            chunks_by_url[link] += f"\n\n{content_chunk}"
                        else:
                            chunks_by_url[link] = content_chunk

                    # Format combined chunks with proper tags for DEFAULT_TEMPLATE
                    chunks = []
                    for link, combined_content in chunks_by_url.items():
                        formatted_chunk = f"<text>{combined_content}</text>"
                        if link:
                            formatted_chunk += f"\n<reference><url>{link}</url></reference>"
                        chunks.append(formatted_chunk)

                    logger.info(f"\n====================================================== Stage 2: Retrieved {len(chunks)} chunk blocks ======================================================")
                    for i, chunk in enumerate(chunks, 1):
                        logger.info(f"\n   {i}. {chunk}{"..." if len(chunk) > 120 else ""}")

                    return chunks

        except Exception as e:
            logger.exception(f"Error in Stage 2 chunk retrieval: {e}")
            return []

    # --------------------------------------------
    # Back-compat Stage-2 method (kept, minimal)
    # --------------------------------------------
    def _stage2_chunk_retrieval(
        self, 
        query: str, 
        query_vector: List[float], 
        document_uuids: List[str]
    ) -> List[str]:
        """
        Backward-compatible wrapper that calls the new Stage-2 with only document UUIDs.
        Prefer using _stage2_chunk_retrieval_fusion with both doc & section UUIDs.
        """
        return self._stage2_chunk_retrieval_fusion(
            query=query,
            query_vector=query_vector,
            document_uuids=document_uuids,
            section_uuids=[],
        )

    # --------------
    # Public API
    # --------------
    def retrieve(self, query: str) -> List[str]:
        """
        Two-stage retrieval process:
        1. Extract keywords and enhance query
        2. Stage 1: Rank-fusion unit filter (documents + sections)
        3. Stage 2: Chunk retrieval with authoritative 70/30 fusion
        """
        retrieval_start = time.time()
        logger.info(f"Starting two-stage retrieval for: {query}")
        try:
            # Step 1: Embed query
            embed_start = time.time()
            #TODO: Research how to enhance query with NER or query expansion
            # enhanced_query = self.ner_extractor.extract_keywords(query)
            enhanced_query = self.query_expander.expand_query(query)
            logger.info(f"Enhanced query: {enhanced_query}")

            query_vector = self._embed_query(query)
            embed_time = time.time() - embed_start

            # Step 2: Stage 1 - Rank fusion (documents + sections)
            stage1_start = time.time()
            doc_units, sec_units = self._stage1_unit_filter(
                enhanced_query, query_vector, include_docs=True, include_sections=True, cap_units=75
            )
            stage1_time = time.time() - stage1_start

            if not doc_units and not sec_units:
                logger.warning("No units found in Stage 1")
                return []

            # Extract UUIDs for Stage 2
            document_uuids = [doc_unit["uuid"] for doc_unit in doc_units]
            section_uuids = [sec_unit["uuid"] for sec_unit in sec_units]

            # Step 3: Stage 2 - Chunk retrieval with 70/30 fusion
            stage2_start = time.time()
            chunks = self._stage2_chunk_retrieval_fusion(
                enhanced_query, query_vector, document_uuids, section_uuids
            )
            stage2_time = time.time() - stage2_start

            # Log retrieval latency
            total_time = time.time() - retrieval_start
            logger.info("")
            logger.info("=" * 60 + " Retrieval Latency " + "=" * 60)
            logger.info(f"Total Retrieval Time: {total_time:.3f}s")
            logger.info(f"  - Embedding:        {embed_time:.3f}s ({embed_time/total_time*100:.1f}%)")
            logger.info(f"  - Stage 1 (Filter): {stage1_time:.3f}s ({stage1_time/total_time*100:.1f}%)")
            logger.info(f"  - Stage 2 (Chunks): {stage2_time:.3f}s ({stage2_time/total_time*100:.1f}%)")
            logger.info(f"  - Other:            {total_time - embed_time - stage1_time - stage2_time:.3f}s")
            logger.info("=" * 140)
            logger.info("")

            return chunks

        except Exception as e:
            logger.error(f"Error in two-stage retrieval: {e}")
            return []

    def __del__(self):
        """Clean up connection pool when object is destroyed."""
        if hasattr(self, "pool"):
            self.pool.close()
