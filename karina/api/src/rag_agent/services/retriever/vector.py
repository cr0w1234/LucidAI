# from typing import Dict, Any, List

# from langchain_openai import OpenAIEmbeddings
# from pinecone import Pinecone
# from pinecone.exceptions.exceptions import NotFoundException
# from langchain_pinecone import PineconeVectorStore

# from functools import cached_property

# from rag_agent.core.config import settings
# from .base_retriever import BaseRetriever


# class PineconeVectorRetriever(BaseRetriever):
#     """
#     A retriever for a Pinecone index using OpenAI embeddings.
#     Caches the retriever for a given search type and k.
#     """
#     def __init__(
#         self,
#         *,
#         top_k: int = 14,
#         embedding_model: str,
#         index_name: str,
#         search_type: str = "similarity",
#     ) -> None:
#         self.embedding_model = embedding_model
#         self.index_name = index_name
#         self.search_type = search_type
#         self.top_k = top_k


#     @cached_property
#     def _openai_embeddings(self) -> OpenAIEmbeddings:
#         """Create an OpenAI embeddings client."""
#         return OpenAIEmbeddings(model=self.embedding_model, 
#             openai_api_key=settings.OPENAI_API_EMBEDDINGS_KEY.get_secret_value()
#         )


#     @cached_property
#     def _pinecone_index(self) -> Any: #Returns a Pinecone Index object
#         """Initialize Pinecone client with a specified index."""
#         pc = Pinecone(api_key=settings.PINECONE_API_KEY.get_secret_value())
#         try:
#             pc.describe_index(self.index_name)
#         except NotFoundException:
#             raise RuntimeError(
#                 f"Index `{self.index_name}` not found. Create it in the Pinecone console."
#             )
#         return pc.Index(self.index_name)
    

#     @cached_property
#     def _langchain_retriever(self) -> BaseRetriever:

#         embed = self._openai_embeddings
#         index = self._pinecone_index
#         text_field = "id"
#         vectorstore = PineconeVectorStore(
#             index,
#             embed,
#             text_field  # metadata field for chunk IDs
#         )
#         return vectorstore.as_retriever(
#             search_type=self.search_type,
#             search_kwargs={"k": self.top_k}
#         )


#     def retrieve(self, query: str) -> List[str]:
#         """
#         Build a LangChain retriever over a Pinecone index using OpenAI embeddings.
#         """
#         retriever = self._langchain_retriever
#         docs = retriever.invoke(query)
#         ids = [doc.page_content for doc in docs]
#         return ids