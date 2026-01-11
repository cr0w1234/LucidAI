#pydantic v2
from typing import List, Union
from uuid import UUID

from pydantic import AnyHttpUrl, field_validator
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from pathlib import Path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="CSHA_",
        extra="ignore",
    )

    APP_NAME: str = "WEBSITE ETL"

    OPENAI_API_EMBEDDINGS_KEY: SecretStr
    OPENAI_API_QUERY_KEY: SecretStr

    EMBEDDING_MODEL: str = "text-embedding-3-large"
    QUERY_MODEL: str = "gpt-4.1-mini"
    
    # Do NOT change these values - they affect the document indexing that chunk UUIDs creation relies on
    CHUNK_SIZE: int = 400
    CHUNK_OVERLAP: int = 15
    # UUID namespaces for document and chunk UUIDs
    DOCUMENT_UUID_NAMESPACE: UUID = UUID("11111111-1111-1111-1111-111111111111")
    CHUNK_UUID_NAMESPACE: UUID = UUID("22222222-2222-2222-2222-222222222222")

    SQL_DIR: Path = Path(__file__).resolve().parent.parent / "load" / "sql"
    PG_RESTORE_BIN: str = "/usr/lib/postgresql/17/bin/pg_restore"
    DSN: str = "postgresql://leonardjin@127.0.0.1:5432/csha_dev_test"
    SQL_TIMEOUT_S: float = 10.0
    
    DUMP_PATH: Path = Path("/tmp/csha_prod_pg16.dump")

    # --- AWS ---
    AWS_SSH_KEY: Path = Path("~/.ssh/LightsailDefaultKey-us-west-2-csha.pem").expanduser()
    AWS_SSH_HOST: str = "ubuntu@52.27.127.130"
    AWS_REMOTE_DB: str = "csha_prod_test"
    AWS_REMOTE_DIR: str = "/tmp"
    # --- Data Directories ---
    BASE_DATA_DIR: Path = Path(__file__).resolve().parent.parent.parent.parent / "data"
    RAW_DATA_DIR: Path = BASE_DATA_DIR / "raw"
    STAGED_DATA_DIR: Path = BASE_DATA_DIR / "staged"
    PROCESSED_DATA_DIR: Path = BASE_DATA_DIR / "processed"

settings = Settings()