import os
import sys
import json
import pickle
import asyncio
from enum import Enum
from typing import List, AsyncGenerator, Dict, Any

from nltk.tokenize import word_tokenize

from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from pinecone import Pinecone
from langchain_pinecone import PineconeVectorStore
from langchain.prompts import PromptTemplate

from ner_keyword_expander import NERKeywordExpander
from prompt_templates.multiple_reference_response_template import *


class RetrievalMethod(Enum):
    """Supported retrieval backends."""
    VECTOR_DB = "vector_db"
    BM25 = "bm25"


class BM25Retriever:
    """Wraps a precomputed BM25 index to return top-N document IDs."""

    def __init__(
        self,
        bm25_index: Any,
        top_n: int,
        ids: List[str]
    ) -> None:
        self.bm25 = bm25_index
        self.top_n = top_n
        self.ids = ids
        self.ner_keyword_extractor = NERKeywordExpander()

    def invoke(self, query: str) -> List[str]:
        """
        Expand the query via NER, tokenize, score against BM25 index,
        and return the top-N chunk IDs.
        """
        expanded = self.ner_keyword_extractor.expand(query)
        tokens = word_tokenize(expanded.lower().replace(",", ""))
        scores = self.bm25.get_scores(tokens)
        # Get indices of top-N scores (highest first)
        top_indices = scores.argsort()[-self.top_n:][::-1]
        return [self.ids[i] for i in top_indices]


def initialize_openai_embeddings(
    model_name: str,
    api_key: str
) -> OpenAIEmbeddings:
    """Create an OpenAI embeddings client."""
    return OpenAIEmbeddings(model=model_name, openai_api_key=api_key)


def initialize_pinecone(
    api_key: str,
    index_name: str
) -> Any:
    """Initialize Pinecone client and return the specified index."""
    pc = Pinecone(api_key=api_key)
    return pc.Index(index_name)


async def stream_llm_responses(
    llm: ChatOpenAI,
    request: str
) -> AsyncGenerator[str, None]:
    """
    Asynchronously stream chunks from the LLM, indenting newlines
    for readability in console output.
    """
    async for chunk in llm.astream(request):
        yield chunk.content.replace("\n", "\n\t")


def get_openai_retriever(
    embedding_model: str,
    openai_api_key: str,
    index_name: str,
    pinecone_api_key: str
) -> Any:
    """
    Build a LangChain retriever over a Pinecone index using OpenAI embeddings.
    """
    embed = initialize_openai_embeddings(embedding_model, openai_api_key)
    index = initialize_pinecone(pinecone_api_key, index_name)
    text_field = "id"
    vectorstore = PineconeVectorStore(
        index,
        embed,
        text_field  # metadata field for chunk IDs
    )
    return vectorstore.as_retriever(
        search_type="similarity",
        search_kwargs={"k": 8}
    )


def get_bm25_retriever(
    data: Dict[str, Any]
) -> BM25Retriever:
    """
    Load or error out if the BM25 index file is missing,
    then return a BM25Retriever over the loaded index.
    """
    path = os.path.dirname(os.path.realpath(__file__))
    index_file = os.path.join(path, "document-data-etl/output/document_index.pkl")
    if not os.path.exists(index_file):
        sys.exit(
            f"Error: index file {index_file} not found. "
            "Please run bm25_tokenizer.py first."
        )

    with open(index_file, "rb") as f:
        bm25_index = pickle.load(f)

    # Build ids: heading IDs first, then subheading IDs, in order
    ids: List[str] = []
    for heading in data.get("headings", []):
        heading_id = heading.get("id")
        if heading_id:
            ids.append(heading_id)
        for sub in heading.get("subheadings", []):
            sub_id = sub.get("id")
            if sub_id:
                ids.append(sub_id)

    return BM25Retriever(bm25_index, top_n=10, ids=ids)


def get_text_by_id(
    chunk_id: str,
    hash_map: Dict[str, Any]
) -> str:
    """
    Retrieve the text and reference information for a given chunk ID 
    from a hierarchical JSON structure consisting of headings and subheadings.
    """
    for heading in hash_map.get("headings", []):
        title = heading.get("heading_title", "").strip()
        if title.lower() == "sources":
            continue  # Skip the "Sources" section

        heading_id = heading.get("id", "")
        if heading_id == chunk_id:
            return (
                f"<text> {heading['text']} </text>\n"
                f"<reference>\n"
                f"  <heading> {heading['heading_title']} </heading>\n"
                f"  <heading_number> {heading['heading_number']} </heading_number>\n"
                f"</reference>"
            )

        for subheading in heading.get("subheadings", []):
            subheading_id = subheading.get("id", "")
            if subheading_id == chunk_id:
                return (
                    f"<text> {subheading['text']} </text>\n"
                    f"<reference>\n"
                    f"  <heading> {subheading['subheading_title']} </heading>\n"
                    f"  <heading_number> {heading['heading_number']} </heading_number>\n"
                    f"  <subheading_number> {subheading['subheading_number']} </subheading_number>\n"
                    f"</reference>"
                )

    return ""


async def handle_query(
    query: str,
    retriever: Any,
    retrieval_method: RetrievalMethod,
    prompt: PromptTemplate,
    llm: ChatOpenAI,
    hash_map: Dict[str, Any]
) -> str:
    """
    Run one retrieval+generation cycle:
    1. Get chunk IDs
    2. Assemble context
    3. Stream LLM response
    """
    ids = retriever.invoke(query)

    # Build context from retrieved chunks
    context_parts: List[str] = []
    for doc in ids:
        chunk_id = (
            doc.page_content
            if retrieval_method is RetrievalMethod.VECTOR_DB
            else doc
        )
        if chunk_id:
            text = get_text_by_id(chunk_id, hash_map)
            if text:
                print(f"Text Document: \n {text} \n")
                context_parts.append(text)

    request = prompt.format(
        context="\n".join(context_parts),
        question=query
    )

    # Stream and collect the response
    print("\n------------- AI AGENT RESPONSE -------------\n")
    response = ""
    async for piece in stream_llm_responses(llm, request):
        print(piece, end="")
        response += piece
    return response


async def retrieval_augmented_generation(
    hash_map: Dict[str, Any],
    retrieval_method: RetrievalMethod,
    openai_query_api_key: str,
    embedding_model: str,
    openai_embedding_api_key: str,
    index_name: str,
    pinecone_api_key: str
) -> None:
    """Main REPL loop for RAG: choose retriever, prompt user, answer."""
    llm = ChatOpenAI(
        openai_api_key=openai_query_api_key,
        model_name="gpt-4.1",
        temperature=0.0,
        streaming=True
    )
    prompt = PromptTemplate(
        input_variables=["context", "question"],
        template=MULTIPLE_REFERENCES_RESPONSE_TEMPLATE
    )

    while True:
        query = input("\nEnter your query (or 'quit' to exit):\n>>> ").strip()
        if not query or query.lower() == "quit":
            break

        if retrieval_method is RetrievalMethod.VECTOR_DB:
            retriever = get_openai_retriever(
                embedding_model,
                openai_embedding_api_key,
                index_name,
                pinecone_api_key
            )
        else:
            retriever = get_bm25_retriever(hash_map)

        await handle_query(
            query,
            retriever,
            retrieval_method,
            prompt,
            llm,
            hash_map
        )


async def main() -> None:
    """Entry point: load data, set method, and launch the RAG loop."""
    # retrieval_method = RetrievalMethod.VECTOR_DB
    retrieval_method = RetrievalMethod.BM25

    OPENAI_API_EMBEDDINGS_KEY = os.environ["OPENAI_API_EMBEDDINGS_KEY"]
    OPENAI_API_QUERY_KEY = os.environ["OPENAI_API_QUERY_KEY"]
    PINECONE_API_KEY = os.environ["PINECONE_API_KEY"]
    embedding_model = "text-embedding-3-large"
    index_name = "attracting-and-retaining-adolescent-patients"

    # Load the pre-structured JSON with IDs
    path = os.path.dirname(os.path.realpath(__file__))
    input_file = os.path.join(path, "document-data-etl/output/structured_text_with_ids.json")
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    await retrieval_augmented_generation(
        data,
        retrieval_method,
        OPENAI_API_QUERY_KEY,
        embedding_model,
        OPENAI_API_EMBEDDINGS_KEY,
        index_name,
        PINECONE_API_KEY
    )


if __name__ == "__main__":
    asyncio.run(main())
