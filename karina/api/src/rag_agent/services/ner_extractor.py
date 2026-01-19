import logging
from typing import List
from rag_agent.core.model_client import get_model_client, ModelConfig, ModelType
from rag_agent.core.prompt_templates.ner_keyword_extractor_template import NER_KEYWORD_EXTRACT_TEMPLATE
from rag_agent.core.config import settings

logger = logging.getLogger(__name__)


class NERKeywordExtractor:
    """Extract keywords and entities from user queries to enhance retrieval."""
    
    def __init__(self, model_name: str = settings.QUERY_MODEL):
        self.model_name = model_name
        self.model_config = ModelConfig(
            model_type=ModelType.QUERY,
            model_name=model_name,
            temperature=0.0
        )
        self.model_client = get_model_client(self.model_config)
    
    def extract_keywords(self, query: str) -> str:
        """
        Extract keywords and entities from a user query using NER and keyword expansion.

        Args:
            query: The user's query string to extract keywords and entities from
            
        Returns:
            Query string with extracted keywords and entities
        """
        try:
            prompt = QUERY_EXPAND_TEMPLATE.format(query=query)
            response = self.model_client.invoke(prompt)
            
            # Extract the content from the response
            if hasattr(response, 'content'):
                enhanced_query = response.content.strip()
            else:
                enhanced_query = str(response).strip()
            
            logger.info(f"NER Keyword Extraction: '{query}' → '{enhanced_query}'")
            
            return enhanced_query
            
        except Exception as e:
            logger.error(f"Error extracting keywords: {e}")
            # Return original query if extraction fails
            return query
