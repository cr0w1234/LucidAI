# import os
# import sys
# import pickle
# from functools import cached_property
# import numpy as np
# from typing import List, Any

# import nltk
# from nltk.tokenize import word_tokenize
# nltk.download('punkt')
# nltk.download('punkt_tab')

# from .base_retriever import BaseRetriever
# from rag_agent.services.generator import Generator
# from rag_agent.core.resources import (
#     load_bm25_model_pkl,
#     load_doc_id_map_npy
# )
# from rag_agent.core.prompt_templates import NER_KEYWORD_EXTRACT_TEMPLATE


# class OkapiBM25Retriever(BaseRetriever):
#     """Wraps a precomputed BM25 index to return top-N document IDs."""

#     def __init__(
#         self,
#         *,
#         top_k: int,
#         query_model: str,
#     ) -> None:
#         self.top_k = top_k
#         self.ner_keyword_expander = Generator(
#             prompt_template=NER_KEYWORD_EXTRACT_TEMPLATE,
#             query_model=query_model
#         )
        

#     def retrieve(self, query: str) -> List[str]:
#         """
#         Expand the query via NER, tokenize, score against BM25 index,
#         and return the top-N chunk IDs.
#         """
#         bm25_model = load_bm25_model_pkl()
#         doc_id_map = load_doc_id_map_npy()
#         expanded = self.ner_keyword_expander.generate(query=query)
#         tokens = word_tokenize(expanded.replace(",", ""))
#         scores = bm25_model.get_scores(tokens) #returns a numpy array of scores for each token in the query
#         # Get indices of top-K scores (highest first)
#         top_indices = scores.argsort()[-self.top_k:][::-1]
#         ids = doc_id_map[top_indices].tolist()
#         # return ids