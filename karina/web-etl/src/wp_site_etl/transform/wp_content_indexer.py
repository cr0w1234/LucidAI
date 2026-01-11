#!/usr/bin/env python3

import json
from typing import Any, List
from pathlib import Path
from uuid import UUID, uuid5

from langchain.prompts import PromptTemplate
from bs4 import BeautifulSoup
import unicodedata
import tiktoken

from wp_site_etl.core.prompt_templates.valid_content_identifier_template import VALID_CONTENT_IDENTIFIER_TEMPLATE
from wp_site_etl.core.prompt_templates.excerpt_generator_template import EXCERPT_GENERATOR_TEMPLATE
from wp_site_etl.core.config import settings
from wp_site_etl.core.model_client import get_model_client, ModelConfig, ModelType

def _is_valid_content(text: str, MODEL_TYPE: ModelType, MODEL_NAME: str) -> bool:
    """
    Queries the LLM and returns a boolean (True/False) depending on whether the text is contains valid content
    """
    model_config = ModelConfig(model_type=MODEL_TYPE, model_name=MODEL_NAME)
    llm = get_model_client(model_config)

    prompt = PromptTemplate(
        input_variables=["text_input"],
        template=VALID_CONTENT_IDENTIFIER_TEMPLATE
    )
    formatted_prompt = prompt.format(text_input=text)
    response = llm.invoke(formatted_prompt).content
    if "true" in response.lower().strip():
        return True
    else:
        return False

# Clean HTML Logic out of rendered content
def clean_rendered_text(content: str) -> str:
    """
    Parses HTML, extracts text from header (h1–h6) and paragraph (p) tags,
    strips control‑category Unicode chars plus \n, \r, and \t,
    and returns an HTML-like string where headings retain their tags
    and paragraphs are plain text lines.
    """

    if not content:
        return ""

    soup = BeautifulSoup(content, 'html.parser')
    
    elements = soup.find_all(['h1','h2','h3','h4','h5','h6','p'])
    cleaned_blocks = []
    
    for el in elements:
        # Extract visible text
        text = el.get_text(separator=' ', strip=True)
        # Remove control chars and explicit newline/tab/carriage returns
        clean = ''.join(
            ch for ch in text
            if not unicodedata.category(ch).startswith('C')
               and ch not in ('\n', '\r', '\t')
        )
        if not clean:
            continue
        
        # Wrap headings in their tag; paragraphs as plain text
        if el.name in ['h1','h2','h3','h4','h5','h6']:
            cleaned_blocks.append(f"<{el.name}>{clean}</{el.name}>")
        else:  # paragraph
            cleaned_blocks.append(clean)
    
    # Join with newline for readability
    return " ".join(cleaned_blocks)


def generate_excerpt(page_content: str, MODEL_TYPE: ModelType, MODEL_NAME: str) -> str:
    """
    Generate a summary excerpt from the given page content using a language model.
    """
    model_config = ModelConfig(model_type=MODEL_TYPE, model_name=MODEL_NAME)
    llm = get_model_client(model_config)

    prompt = PromptTemplate(
        input_variables=["text_input"],
        template=EXCERPT_GENERATOR_TEMPLATE
    )

    formatted_prompt = prompt.format(text_input=page_content)

    response = llm.invoke(formatted_prompt).content
    return response

def create_document_uuid(NAMESPACE: UUID, page_id: int) -> str:
    """
    Generates a UUID for a document based on the page ID and namespace.
    """
    return str(uuid5(NAMESPACE, f"{page_id}"))

def create_chunk_uuid(NAMESPACE: UUID, document_uuid: str, chunk_index: int) -> str:
    """
    Generates a UUID for a chunk based on the document UUID and chunk index.
    """
    return str(uuid5(NAMESPACE, f"{document_uuid}:{chunk_index}"))


def chunk_content_by_tokens(
    text: str,
    chunk_size: int,
    chunk_overlap: int,
    model_name: str = "gpt-4o-mini",
    min_tail_tokens: int = 50,
) -> List[str]:
    """
    Chunk `text` by *tokens* (not words), with overlap.
    
    - chunk_size: max tokens per chunk
    - chunk_overlap: how many tokens to repeat between consecutive chunks
    - min_tail_tokens: if the final remainder is smaller than this, merge it into the previous chunk
    """

    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if not (0 <= chunk_overlap < chunk_size):
        raise ValueError("chunk_overlap must be >= 0 and < chunk_size")

    enc = tiktoken.encoding_for_model(model_name)
    token_ids = enc.encode(text)

    step = chunk_size - chunk_overlap
    chunks: List[str] = []

    i = 0
    n = len(token_ids)

    while i < n:
        remaining = n - i

        # If what's left can't form a full chunk, merge it into the previous chunk.
        if remaining < chunk_size and chunks:
            prev_ids = enc.encode(chunks[-1])
            merged_ids = prev_ids + token_ids[i:n]
            chunks[-1] = enc.decode(merged_ids)
            break

        # If this is the first chunk and the whole doc is smaller than chunk_size, just return it.
        if remaining < chunk_size and not chunks:
            chunks.append(enc.decode(token_ids[i:n]))
            break

        end = i + chunk_size
        chunks.append(enc.decode(token_ids[i:end]))
        i += step

    return chunks



def build_tree(
    pages: list[dict[str, Any]], 
    rotten_ids: list[int], 
    MODEL_TYPE: ModelType, 
    MODEL_NAME: str,
    CHUNK_SIZE: int,
    CHUNK_OVERLAP: int,
    DOCUMENT_UUID_NAMESPACE: UUID, 
    CHUNK_UUID_NAMESPACE: UUID
) -> tuple[dict[str, Any], List[dict[str, Any]], List[dict[str, Any]]]:
    """
    Builds a hierarchical tree structure JSON, flattend JSONL, and flattend JSONL document chunks from raw webpage JSON data with parent-child relationships.

    Args:
        pages (list[dict[str, Any]]): Flat list of pages with parent-child relationships.
        rotten_ids (list[int]): List of page IDs that are rotten.
        MODEL_TYPE (ModelType): Model type to use for the LLM. This can be either "query" or "embedding".
        MODEL_NAME (str): Model name to use for the LLM.
        CHUNK_SIZE (int): Size of each chunk.
        CHUNK_OVERLAP (int): Overlap between chunks.
        DOCUMENT_UUID_NAMESPACE (UUID): Namespace for document UUIDs.
        CHUNK_UUID_NAMESPACE (UUID): Namespace for chunk UUIDs.

    Returns:
        tuple[dict[str, Any], List[dict[str, Any]], List[dict[str, Any]]]: Tuple containing the hierarchical tree structure JSON, flattend JSONL, and flattend JSONL document chunks.
    """

    # print("Number of pages: ", len(pages))
    # Create a dictionary of simplified nodes indexed by their ID
    json_nodes: dict[int, dict[str, Any]] = {}
    jsonl_nodes: dict[int, dict[str, Any]] = {}
    
    jsonl_document_chunks_output_data: List[dict[str, Any]] = []

    empty_pages: dict[int, str] = {}
    empty_pages_count = 0
    special_pages: dict[int, str] = {}
    special_pages_count = 0
    invalid_content_pages: dict[int, str] = {}
    invalid_content_pages_count = 0

    for page in pages:
        rendered_content = page.get('content', {}).get('rendered', '') 
        rendered_excerpt = page.get('excerpt', {}).get('rendered', '')
        content = clean_rendered_text(rendered_content)
        excerpt = clean_rendered_text(rendered_excerpt)

        if not rendered_content:
            print(f"Content has not been published to site for {page['title']['rendered']} webpage.")
            empty_pages[page['id']] = page['link']
            rotten_ids.append(page['id'])
            empty_pages_count += 1
            continue
        elif not rendered_excerpt:
            #I have noticed that if the page is missing an excerpt its usually because its content isn't published, psuedo latin, or its a special page
            if _is_valid_content(content, MODEL_TYPE, MODEL_NAME): 
                generated_excerpt = generate_excerpt(content, MODEL_TYPE, MODEL_NAME)
            else:
                print("\nNot valid CONTENT. Skip...")
                print("Not valid content: ", content, "\n")
                invalid_content_pages[page['id']] = page['link']
                rotten_ids.append(page['id'])
                invalid_content_pages_count += 1
                continue
        elif not _is_valid_content(excerpt, MODEL_TYPE, MODEL_NAME):
            print("\nNot valid EXCERPT. Skip...")
            print("Not valid excerpt: ", excerpt)
            print("Not valid content: ", content, "\n")
            invalid_content_pages[page['id']] = page['link']
            rotten_ids.append(page['id'])
            invalid_content_pages_count += 1
            continue
        else:
            if not content:
                print("Does not have paragraph tags <p></p>. This is a special page (ie. contains only hyperlinks, special paragraph tags like excerpts, etc). Manually add content.")
                special_pages[page['id']] = page['link']
                rotten_ids.append(page['id'])
                special_pages_count += 1
                continue

        # Cannonical Webpage document UUID
        document_uuid = create_document_uuid(DOCUMENT_UUID_NAMESPACE, page['id'])

        # PostgreSQL - Document full-text and summary table
        jsonl_node = {
            'page_id': page['id'],
            'title': page.get('title', {}).get('rendered', ''),
            'link': page['link'],
            'content': content,
            'modified': page['modified'],
            'slug': page['slug'],
            'status': page['status'],
            'excerpt': excerpt,
            'document_uuid': document_uuid,
            'document_parent_uuid': None # `document_uuid` of parent page
        }
        jsonl_nodes[page['id']] = jsonl_node

        # PostgreSQL - Document chunks table
        chunks = chunk_content(content, CHUNK_SIZE, CHUNK_OVERLAP)

        # Append each chunk directly to the output data because the document-chunks relationship is already captured by the `document_uuid`
        for idx,chunk in enumerate(chunks):
            chunk_uuid = create_chunk_uuid(CHUNK_UUID_NAMESPACE, document_uuid, idx)
            jsonl_document_chunks_output_data.append({
                'content_chunk': chunk,
                'chunk_uuid': chunk_uuid,
                'chunk_index': idx, # Capture the order of the chunks
                'document_uuid': document_uuid
            })

        #Debugging - Nested JSON website data (Source of truth)
        json_node = {
            'page_id': page['id'],
            'title': page.get('title', {}).get('rendered', ''),
            'link': page['link'],
            'content': {'full_text': content, 'chunks': chunks},
            'modified': page['modified'],
            'slug': page['slug'],
            'status': page['status'],
            'excerpt': rendered_excerpt,
            'children': [],
            'document_uuid': document_uuid # `document_uuid` of parent page
        }
        json_nodes[page['id']] = json_node

    json_output_data = {
        'empty_pages_count': empty_pages_count,
        "empty_pages": empty_pages,
        'special_pages_count': special_pages_count,
        'special_pages': special_pages,
        'webpage_tree': [],  # will hold the actual page nodes
    }

    # Populate the tree
    for page in pages:
        if page['id'] not in rotten_ids:
            json_node = json_nodes[page['id']]
            jsonl_node = jsonl_nodes[page['id']]
            
            parent_id = page.get('parent', 0) or 0
            if parent_id and parent_id in json_nodes:
                json_nodes[parent_id]['children'].append(json_node)
                # Capture parent-child document relationship
                jsonl_node['document_parent_uuid'] = jsonl_nodes[parent_id]['document_uuid']
            else:
                json_output_data['webpage_tree'].append(json_node)

    # Create a list where each element in the list is a JSONL node representing a webpage and its metadata
    jsonl_document_output_data = list(jsonl_nodes.values())

    return (json_output_data, jsonl_document_output_data, jsonl_document_chunks_output_data)


def main():

    RAW_DATA_DIR = settings.RAW_DATA_DIR / "website-data" / "endpoint-content"
    STAGED_DATA_DIR = settings.STAGED_DATA_DIR

    MODEL_TYPE = ModelType.QUERY
    MODEL_NAME = settings.QUERY_MODEL

    DOCUMENT_UUID_NAMESPACE = settings.DOCUMENT_UUID_NAMESPACE
    CHUNK_UUID_NAMESPACE = settings.CHUNK_UUID_NAMESPACE

    CHUNK_SIZE = settings.CHUNK_SIZE
    CHUNK_OVERLAP = settings.CHUNK_OVERLAP

    combined_json_document_data = []
    combined_jsonl_document_data = []
    combined_jsonl_document_chunks_data = []
    rotten_ids = []
    for path in RAW_DATA_DIR.rglob('*.json'):
        print("Processing: ", path)
        with path.open('r', encoding='utf-8') as file:
            data = json.load(file, strict=False) # Hack - fix later
            json_output_data, jsonl_document_output_data, jsonl_document_chunks_output_data = build_tree(
                data, 
                rotten_ids, 
                MODEL_TYPE, 
                MODEL_NAME, 
                CHUNK_SIZE, 
                CHUNK_OVERLAP, 
                DOCUMENT_UUID_NAMESPACE, 
                CHUNK_UUID_NAMESPACE
            )
            # Save the JSON output of each endpoint in staged data directory
            with open(STAGED_DATA_DIR / f"indexed_{path.stem}_data.json", 'w', encoding='utf-8') as file:
                json.dump(json_output_data, file, ensure_ascii=False, indent=2)
            print("Adding to combined data: ", path.stem, "\n")

            combined_json_document_data.extend(json_output_data.get('webpage_tree', []))

            combined_jsonl_document_data.extend(jsonl_document_output_data)
            combined_jsonl_document_chunks_data.extend(jsonl_document_chunks_output_data)
    
    print("Number of rotten IDs: ", len(rotten_ids))
    # print("Rotten IDs: ", rotten_ids)

    with open(STAGED_DATA_DIR / f"website_data.json", 'w', encoding='utf-8') as file:
        json.dump(combined_json_document_data, file, ensure_ascii=False, indent=2)

    with open(STAGED_DATA_DIR / "documents.jsonl", "w", encoding="utf-8") as out:
        for page in combined_jsonl_document_data:
            out.write(json.dumps(page, ensure_ascii=False) + "\n")

    with open(STAGED_DATA_DIR / "document_chunks.jsonl", "w", encoding="utf-8") as out:
        for page in combined_jsonl_document_chunks_data:
            out.write(json.dumps(page, ensure_ascii=False) + "\n")


if __name__ == '__main__':
    main()
