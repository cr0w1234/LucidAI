import logging
import rag_agent.core.logging_config
logger = logging.getLogger(__name__)

import asyncio
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, AsyncGenerator
from langchain_openai import ChatOpenAI

from rag_agent.core.config import settings, Settings
from rag_agent.services.retriever.base_retriever import BaseRetriever
from rag_agent.services.retriever.hybrid import HybridRetriever
from rag_agent.services.retriever.two_stage import TwoStageRetriever
from rag_agent.core.model_client import get_model_client, ModelConfig, ModelType
from rag_agent.core.enums import RetrievalMethod

from rag_agent.core.prompt_templates import DEFAULT_TEMPLATE


def get_context(
    query: str,
    retriever: BaseRetriever,
) -> List[str]:
    """Get the context for the query."""
    return retriever.retrieve(query)


async def handle_query(
    query: str,
    retriever: BaseRetriever,
    model_client: ChatOpenAI,
    prompt_template: str
) -> AsyncGenerator[str, None]:
    """
    Run one retrieval+generation cycle:
    1. Get context
    2. Stream LLM response
    """
    handle_start = time.time()
    
    # Retrieval (timing already logged in retriever.retrieve())
    retrieval_start = time.time()
    context_parts = get_context(query, retriever)
    retrieval_time = time.time() - retrieval_start
    
    if not context_parts:
        logger.warning("No context parts found for query: %s", query)
        return

    # Build prompt
    prompt_start = time.time()
    context = " ".join(context_parts).replace("\n", "\n\t")

    now = datetime.now(ZoneInfo("America/Los_Angeles"))
    today_date = now.strftime("%Y-%m-%d") + " (Pacific Time)"
    logger.info(f"Today's date: {today_date}")
    prompt = prompt_template.format(query=query, context=context, today_date=today_date)
    prompt_time = time.time() - prompt_start
    
    # Generation/AI Response
    generation_start = time.time()
    response = ""
    #For logging the OpenAI response object
    openai_langchain_response = None
    async for response_chunk in model_client.astream(prompt, stream_usage=True):
        try:
            response += response_chunk.content
        except Exception as e:
            logger.error(f"Error adding response chunk. Response chunk may not contain content attribute: {e}")
            continue

        #For logging the OpenAI response object
        openai_langchain_response = response_chunk if openai_langchain_response is None else openai_langchain_response + response_chunk
        
        yield response_chunk.content
    
    generation_time = time.time() - generation_start
    total_handle_time = time.time() - handle_start
    
    logger.info(f"Response generated ({len(response)} characters):")
    logger.info(f"\n\n=========================== Response ===========================\n{response}")
    usage_metadata = openai_langchain_response.usage_metadata
    logger.info(f"\n\n=========================== Response Metadata ===========================\n{usage_metadata}")
    logger.info(f"\n\n=========================== Total Cost ===========================\n${(usage_metadata["input_tokens"] - usage_metadata["input_token_details"]["cache_read"]) * settings.QUERY_MODEL_PRICE_PER_INPUT_TOKEN + (usage_metadata["output_tokens"]) * settings.QUERY_MODEL_PRICE_PER_OUTPUT_TOKEN}")
    
    # Log generation latency
    logger.info("")
    logger.info("=" * 60 + " Generation Latency " + "=" * 60)
    logger.info(f"Total Generation Time: {generation_time:.3f}s")
    logger.info(f"  - First Token Latency: N/A (streaming)")
    logger.info(f"  - Tokens Generated: {usage_metadata.get('output_tokens', 'N/A')}")
    if usage_metadata.get('output_tokens'):
        tokens_per_sec = usage_metadata['output_tokens'] / generation_time
        logger.info(f"  - Generation Speed: {tokens_per_sec:.1f} tokens/sec")
    logger.info("=" * 140)
    logger.info("")
    
    # Log total handle_query latency
    logger.info("")
    logger.info("=" * 60 + " Total Query Latency " + "=" * 60)
    logger.info(f"Total Query Time: {total_handle_time:.3f}s")
    logger.info(f"  - Retrieval:         {retrieval_time:.3f}s ({retrieval_time/total_handle_time*100:.1f}%)")
    logger.info(f"  - Prompt Building:   {prompt_time:.3f}s ({prompt_time/total_handle_time*100:.1f}%)")
    logger.info(f"  - Generation:        {generation_time:.3f}s ({generation_time/total_handle_time*100:.1f}%)")
    logger.info(f"  - Other:             {total_handle_time - retrieval_time - prompt_time - generation_time:.3f}s")
    logger.info("=" * 140)
    logger.info("")

def make_retriever(
    *,
    method: RetrievalMethod,
    DSN: str,
    TOP_K: int,
    SQL_TIMEOUT_S: float,
    VECTOR_WEIGHT: float,
    KEYWORD_WEIGHT: float,
) -> BaseRetriever:
    """Return a retriever based on the method."""
    if method == RetrievalMethod.HYBRID:
        return HybridRetriever(
            dsn=DSN,
            top_k=TOP_K,
            sql_timeout_s=SQL_TIMEOUT_S,
            vector_weight=VECTOR_WEIGHT,
            keyword_weight=KEYWORD_WEIGHT
        )
    elif method == RetrievalMethod.TWO_STAGE:
        return TwoStageRetriever(
            dsn=DSN,
            top_k=TOP_K,  # Stage 2: final chunks
            sql_timeout_s=SQL_TIMEOUT_S,
            vector_weight=VECTOR_WEIGHT,
            keyword_weight=KEYWORD_WEIGHT
        )
    elif method is RetrievalMethod.KEYWORD or method is RetrievalMethod.VECTOR:
        raise ValueError(f"{method} retreival method has not been implemented yet.") 
    else:
        raise ValueError(f"Unsupported retrieval method: {method}")


async def retrieval_augmented_generation(query: str) -> AsyncGenerator[str, None]:
    rag_start = time.time()
    
    # Get the model client directly
    setup_start = time.time()
    model_config = ModelConfig(
        model_type=ModelType.QUERY,
        model_name=settings.QUERY_MODEL,
        streaming=True
    )
    model_client = get_model_client(model_config)

    retriever = make_retriever(
            method=settings.RETRIEVAL_METHOD,
            DSN=settings.DSN,
            TOP_K=settings.TOP_K,
            SQL_TIMEOUT_S=settings.SQL_TIMEOUT_S,
            VECTOR_WEIGHT=settings.VECTOR_WEIGHT,
            KEYWORD_WEIGHT=settings.KEYWORD_WEIGHT
    )
    setup_time = time.time() - setup_start

    async for response_chunk in handle_query(
        query,
        retriever,
        model_client,
        DEFAULT_TEMPLATE
    ):
        yield response_chunk
    
    total_rag_time = time.time() - rag_start
    logger.info("")
    logger.info("=" * 60 + " Total RAG Pipeline Latency " + "=" * 60)
    logger.info(f"Total RAG Time: {total_rag_time:.3f}s")
    logger.info(f"  - Setup:             {setup_time:.3f}s ({setup_time/total_rag_time*100:.1f}%)")
    logger.info(f"  - Query Processing:  {total_rag_time - setup_time:.3f}s ({(total_rag_time - setup_time)/total_rag_time*100:.1f}%)")
    logger.info("=" * 140)
    logger.info("")


async def main() -> None:
    """CLI REPL: read a query and log response."""
    while True:
        raw_query = await asyncio.to_thread(
            input, "\nEnter your query (or 'quit' to exit):\n>>> "
        )
        query = raw_query.strip()
        if not query or query.lower() == "quit":
            break

        # Process the query and let logging handle the output
        logger.info(f"\n\n====================================================== STARTING QUERY ======================================================\n\n")
        async for response_chunk in retrieval_augmented_generation(query):
            pass  # Just consume the chunks, logging will show the full response


if __name__ == "__main__":
    asyncio.run(main())