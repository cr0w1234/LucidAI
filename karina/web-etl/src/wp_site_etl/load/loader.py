import psycopg
import psycopg_pool
import subprocess
from pathlib import Path
from wp_site_etl.core.config import settings
from wp_site_etl.load.exporter import make_dump, scp_upload, remote_restore

def copy_to_staging_table(pool: psycopg_pool.ConnectionPool, table_name: str, file_path: Path):
    """Copy data from file to staging table using COPY command"""
    with pool.connection() as conn:
        with conn.cursor() as cursor:
            with open(file_path, 'r') as file:
                cursor.copy(
                    f"COPY staging.{table_name} (doc) FROM STDIN WITH (FORMAT text)",
                    file
                )
            conn.commit()


def clear_staging_table(pool: psycopg_pool.ConnectionPool, table_name: str):
    """Clear staging table"""
    with pool.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE staging.{table_name}")
            conn.commit()


def stage1_clear_and_load_staging_tables(
    pool: psycopg_pool.ConnectionPool,
    staged_data_dir: Path,
    processed_data_dir: Path,
):

    clear_staging_table(pool, "documents_raw")
    clear_staging_table(pool, "document_chunks_raw")
    clear_staging_table(pool, "document_excerpt_embeddings_3072_raw")
    clear_staging_table(pool, "document_chunks_embeddings_3072_raw")

    copy_to_staging_table(pool, "documents_raw", staged_data_dir / "website_data_documents.jsonl")
    copy_to_staging_table(pool, "document_chunks_raw", staged_data_dir / "website_data_document_chunks.jsonl")
    copy_to_staging_table(pool, "document_excerpt_embeddings_3072_raw", processed_data_dir / "document_embeddings.jsonl")
    copy_to_staging_table(pool, "document_chunks_embeddings_3072_raw", processed_data_dir / "document_chunks_embeddings.jsonl")


def stage2_upsert_prod_tables(pool: psycopg_pool.ConnectionPool):
    """Execute the prod_load.sql commands to upsert data into production tables"""
    
    # Read the SQL file
    prod_load_sql_path = settings.SQL_DIR / "prod_load.sql"
    sql_content = open(prod_load_sql_path).read()
    
    with pool.connection() as conn:
        with conn.cursor() as cursor:
            print("Executing prod_load.sql...")
            cursor.execute(sql_content)
            conn.commit()
    
    print("Production tables updated successfully")


def stage3_generate_dump(
    dsn: str,
    dump_path: Path,
    aws_ssh_key: str,
    aws_ssh_host: str,
    aws_remote_dir: str,
    aws_remote_db: str,
    pg_restore_bin: str,
) -> None:
    dump_file = make_dump(dsn, dump_path)
    uploaded = scp_upload(dump_file, aws_ssh_key, aws_ssh_host, aws_remote_dir)

    remote_path = f"{aws_remote_dir}/{dump_file.name}"
    remote_restore(remote_path, aws_ssh_key, aws_ssh_host, aws_remote_db, pg_restore_bin)


def main():

    staged_data_dir = settings.STAGED_DATA_DIR
    processed_data_dir = settings.PROCESSED_DATA_DIR

    dsn = settings.DSN
    sql_timeout_s = settings.SQL_TIMEOUT_S
    pool = psycopg_pool.ConnectionPool(
        dsn,
        min_size=1,
        max_size=3,
        timeout=sql_timeout_s
    )

    stage1_clear_and_load_staging_tables(pool, staged_data_dir, processed_data_dir)
    stage2_upsert_prod_tables(pool)

    aws_ssh_key = settings.AWS_SSH_KEY
    aws_ssh_host = settings.AWS_SSH_HOST
    aws_remote_dir = settings.AWS_REMOTE_DIR
    aws_remote_db = settings.AWS_REMOTE_DB
    
    pg_restore_bin = settings.PG_RESTORE_BIN

    dump_path = settings.DUMP_PATH
    
    stage3_generate_dump(dsn, dump_path, aws_ssh_key, aws_ssh_host, aws_remote_dir, aws_remote_db, pg_restore_bin)

if __name__ == "__main__":
    main()