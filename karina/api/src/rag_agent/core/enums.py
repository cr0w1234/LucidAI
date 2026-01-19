from enum import Enum


class RetrievalMethod(Enum):
    """Supported retrieval backends."""
    # VECTOR = "vector"
    # KEYWORD = "keyword"
    HYBRID = "hybrid"
    TWO_STAGE = "two_stage"


class ModelType(str, Enum):
    QUERY = "query"
    EMBEDDING = "embedding"