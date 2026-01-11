from enum import Enum

class ModelType(str, Enum):
    QUERY = "query"
    EMBEDDING = "embedding"