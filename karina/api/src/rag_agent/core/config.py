#pydantic v2
from typing import List, Union
from pydantic import AnyHttpUrl, field_validator
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from rag_agent.core.enums import RetrievalMethod

from pathlib import Path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="CSHA_",
        extra="ignore",
    )
    
    APP_NAME: str = "RAG AGENT API"

    # --- API KEYS ---
    BACKEND_API_KEY: SecretStr

    OPENAI_API_EMBEDDINGS_KEY: SecretStr
    OPENAI_API_QUERY_KEY: SecretStr

    # --- MODELS ---
    EMBEDDING_MODEL: str = "text-embedding-3-large"
    QUERY_MODEL: str = "gpt-4.1-mini"

    # --- RETRIEVAL ---
    RETRIEVAL_METHOD: RetrievalMethod = RetrievalMethod.TWO_STAGE
    TOP_K: int = 16
    SQL_TIMEOUT_S: float = 10.0
    VECTOR_WEIGHT: float = 0.7
    KEYWORD_WEIGHT: float = 0.3

    SQL_DIR: Path = Path(__file__).resolve().parent.parent / "services" / "retriever" / "sql"
    # DSN: str = "postgresql:///csha_dev"
    DSN: str = "postgresql://csha_app:csha2025@127.0.0.1:5433/csha_prod"
    
    # --- LOGGING ---
    # Price per token for the current model being used in this application
    QUERY_MODEL_PRICE_PER_INPUT_TOKEN: float = 0.0000004
    QUERY_MODEL_PRICE_PER_OUTPUT_TOKEN: float = 0.0000016

settings = Settings()