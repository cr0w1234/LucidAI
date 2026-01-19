import psycopg
from rag_agent.core.model_client import get_model_client, ModelConfig, ModelType
from rag_agent.core.config import settings
from rag_agent.services.retriever.base_retriever import BaseRetriever
from typing import List

class HybridRetriever(BaseRetriever):
    def __init__(
        self,
        dsn: str,
        top_k: int,
        sql_timeout_s: float,
        vector_weight: float,
        keyword_weight: float,
    ):
        self.dsn = dsn
        self.top_k = top_k
        self.sql_timeout_s = sql_timeout_s
        self.vector_weight = vector_weight
        self.keyword_weight = keyword_weight

        self.embedding_model = settings.EMBEDDING_MODEL
        self.model_type = ModelType.EMBEDDING


    def _embed_query(self, query: str):
        query_vector_config = ModelConfig(
            model_type=self.model_type,
            model_name=self.embedding_model
        )
        return get_model_client(query_vector_config).embed_query(query)


    def retrieve(self, query: str) -> List[str]:

        sql_query = (
            open(settings.SQL_DIR / "hybrid_query.sql").read()
            .replace("%TIMEOUT_MS%", str(int(self.sql_timeout_s * 1000)))
            .replace("%TOP_K%", str(self.top_k))
        )

        query_vector = self._embed_query(query)  # -> list[float] of length 3072

        with psycopg.connect(self.dsn) as db_connection:
            with db_connection.cursor() as db_cursor:
                # First execute the SET LOCAL command
                db_cursor.execute(f"SET LOCAL statement_timeout = {int(self.sql_timeout_s * 1000)};")
                
                # Then execute the main query (the entire sql_query since we removed SET LOCAL from the file)
                db_cursor.execute(
                    sql_query,
                    {
                        'query': query,
                        'vector': query_vector,
                        'vector_weight': self.vector_weight,
                        'keyword_weight': self.keyword_weight
                    }
                )
                rows = db_cursor.fetchall()

        
        return [row[0] for row in rows]
