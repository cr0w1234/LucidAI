#pydantic v2
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="CSHA_",
        extra="ignore",
    )
    
    # --- API KEYS ---
    BACKEND_API_KEY: SecretStr

    OPENAI_API_EMBEDDINGS_KEY: SecretStr
    OPENAI_API_QUERY_KEY: SecretStr

settings = Settings()