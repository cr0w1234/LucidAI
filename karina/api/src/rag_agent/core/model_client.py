from dataclasses import dataclass
from functools import lru_cache
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from typing import Union

from rag_agent.core.config import settings
from rag_agent.core.enums import ModelType


@dataclass (frozen=True)
class ModelConfig:
    model_type: ModelType
    model_name: str
    temperature: float = 0.0
    streaming: bool = False


@lru_cache(maxsize=None)
def get_model_client(config: ModelConfig) -> Union[ChatOpenAI, OpenAIEmbeddings]:
    if config.model_type == ModelType.QUERY:
        api_key = settings.OPENAI_API_QUERY_KEY.get_secret_value()
        if not api_key:
            raise EnvironmentError("CSHA_OPENAI_API_QUERY_KEY not set in environment variables")

        return ChatOpenAI(
        openai_api_key=api_key,
        model_name=config.model_name,
        temperature=config.temperature,
        streaming=config.streaming
    )
    elif config.model_type == ModelType.EMBEDDING:
        api_key = settings.OPENAI_API_EMBEDDINGS_KEY.get_secret_value()
        if not api_key:
            raise EnvironmentError("CSHA_OPENAI_API_EMBEDDINGS_KEY not set in environment variables")

        return OpenAIEmbeddings(
            openai_api_key=api_key,
            model=config.model_name
        )
    else:
        raise ValueError(f"Invalid model type: {config.model_type}")
    