from typing import List
import json

from langchain_openai import OpenAIEmbeddings

from wp_site_etl.core.model_client import get_model_client, ModelConfig, ModelType
from wp_site_etl.core.config import settings

def text_to_embedding(
    text: str,
    embed: OpenAIEmbeddings,
) -> List[float]:
    """
    Create an embedding from the given text.
    
    Args:
        text: The input text to embed.
        embed: Embedding model instance.
    """
    
    embeddings = embed.embed_documents([text])
    print("Embeddings: ", embeddings)
    return embeddings[0]

def create_document_embedding_index(MODEL_TYPE: ModelType, MODEL_NAME: str, row: dict) -> None:
    model_config = ModelConfig(model_type=MODEL_TYPE, model_name=MODEL_NAME)
    embedding_model = get_model_client(model_config)

    embedding = text_to_embedding(row['excerpt'], embedding_model)
    embedding_node = {
        "document_uuid": row['document_uuid'], 
        "model_name": MODEL_NAME, 
        "embedding": embedding
    }

    return embedding_node

def create_document_chunk_embedding_index(MODEL_TYPE: ModelType, MODEL_NAME: str, row: dict) -> None:
    model_config = ModelConfig(model_type=MODEL_TYPE, model_name=MODEL_NAME)
    embedding_model = get_model_client(model_config)

    embedding = text_to_embedding(row['content_chunk'], embedding_model)
    embedding_node = {
        "chunk_uuid": row['chunk_uuid'], 
        "model_name": MODEL_NAME, 
        "embedding": embedding
    }

    return embedding_node
    

def main() -> None:

    MODEL_TYPE = ModelType.EMBEDDING
    MODEL_NAME = settings.EMBEDDING_MODEL

    STAGED_DATA_DIR = settings.STAGED_DATA_DIR
    PROCESSED_DATA_DIR = settings.PROCESSED_DATA_DIR
    
    with open(STAGED_DATA_DIR / "documents.jsonl", "r", encoding="utf-8") as file:
        with open(PROCESSED_DATA_DIR / "document_excerpt_embeddings.jsonl", "w", encoding="utf-8") as output_file:
            for line in file:
                row = json.loads(line.strip())
                document_embedding_node = create_document_embedding_index(MODEL_TYPE, MODEL_NAME, row)
                output_file.write(json.dumps(document_embedding_node) + "\n")

    with open(STAGED_DATA_DIR / "document_chunks.jsonl", "r", encoding="utf-8") as file:
        with open(PROCESSED_DATA_DIR / "document_chunks_embeddings.jsonl", "w", encoding="utf-8") as output_file:
            for line in file:
                row = json.loads(line.strip())
                document_chunk_embedding_node = create_document_chunk_embedding_index(MODEL_TYPE, MODEL_NAME, row)
                output_file.write(json.dumps(document_chunk_embedding_node) + "\n")


if __name__ == "__main__":
    main()