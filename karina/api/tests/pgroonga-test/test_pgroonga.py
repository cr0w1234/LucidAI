#!/usr/bin/env python3
import sys
import psycopg
from pathlib import Path

# Add parent directories to path to import config
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from rag_agent.core.config import settings


def test_pgroonga_extension(conn):
    """Check if pgroonga extension is installed."""
    print("\n" + "=" * 80)
    print("1. Checking pgroonga extension...")
    print("=" * 80)

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM pg_extension WHERE extname = 'pgroonga';")
        result = cur.fetchone()
        if result:
            print(f"✓ pgroonga extension is installed: {result}")
            return True
        else:
            print("✗ pgroonga extension is NOT installed!")
            return False


def test_table_counts(conn):
    """Check document and section counts."""
    print("\n" + "=" * 80)
    print("2. Checking table counts...")
    print("=" * 80)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM prod.documents;")
        doc_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM prod.sections;")
        sec_count = cur.fetchone()[0]
        print(f"  Documents: {doc_count}")
        print(f"  Sections: {sec_count}")
        return doc_count, sec_count


def test_pgroonga_indexes(conn):
    """Check if pgroonga indexes exist."""
    print("\n" + "=" * 80)
    print("3. Checking pgroonga indexes...")
    print("=" * 80)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT indexname, indexdef 
            FROM pg_indexes 
            WHERE schemaname = 'prod' 
              AND tablename IN ('documents', 'sections')
              AND indexdef LIKE '%pgroonga%';
            """
        )
        indexes = cur.fetchall()
        if indexes:
            print(f"✓ Found {len(indexes)} pgroonga index(es):")
            for idx_name, idx_def in indexes:
                print(f"  - {idx_name}")
                print(f"    {idx_def[:100]}...")
            return True
        else:
            print("✗ NO pgroonga indexes found!")
            return False


def run_keyword_query(conn, query_text: str, limit: int = 10) -> None:
    """
    Run a pure keyword PGroonga query against prod.documents and print results.

    query_text comes directly from the CLI (joined sys.argv[1:]).
    """
    print("\n" + "=" * 80)
    print(f"4. Running keyword search for: {query_text!r}")
    print("=" * 80)

    sql = """
        SELECT
            title,
            pgroonga_score(tableoid, ctid) AS score,
            LEFT(content, 200) AS preview
        FROM prod.documents
        WHERE content &@ %(query)s
        ORDER BY score DESC
        LIMIT %(limit)s;
    """

    with conn.cursor() as cur:
        cur.execute(sql, {"query": query_text, "limit": limit})
        rows = cur.fetchall()

    if not rows:
        print("No documents matched this query.")
        return

    for i, (title, score, preview) in enumerate(rows, start=1):
        print(f"\n--- Result {i} ---")
        print(f"Title : {title}")
        print(f"Score : {score}")
        print(f"Preview:\n{preview}")


def main() -> int:
    """Run pgroonga checks and a keyword search based on CLI query."""
    if len(sys.argv) < 2:
        print("Usage: python test_pgroonga.py '<search query>'")
        return 1

    # Everything after the script name is treated as the search query
    query_text = " ".join(sys.argv[1:])

    DSN: str = "postgresql://leonardjin@127.0.0.1:5432/csha_dev"
    conn = psycopg.connect(DSN)

    try:
        test_pgroonga_extension(conn)
        test_table_counts(conn)
        test_pgroonga_indexes(conn)
        run_keyword_query(conn, query_text)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())