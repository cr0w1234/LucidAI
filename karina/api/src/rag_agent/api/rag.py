import time
import json
import asyncio
from typing import Dict, Any, AsyncGenerator

from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.responses import StreamingResponse

from rag_agent.core.config import settings
from rag_agent.schemas.rag import RAGQueryRequest
from rag_agent.services.rag import retrieval_augmented_generation
from rag_agent.security.auth import authenticate_api_key

router = APIRouter(
    prefix="/rag", 
    tags=["RAG"],
    dependencies=[Depends(authenticate_api_key)]
)

def _one_line_json(response_line: Dict[str, Any]) -> bytes:
    return (json.dumps(response_line, separators=(",", ":")) + "\n").encode("utf-8")

@router.post("/query")
async def ndjson_query(body: RAGQueryRequest, request: Request):
    request_id = request.headers.get("x-request-id") or f"rq_{int(time.time() * 1000)}" #request id is a unique identifier for the user request call to the API. Will be needed for logging and debugging (when we implement it).

    async def generate_ndjson_response() -> AsyncGenerator[bytes, None]:
        start_time = time.perf_counter()

        yield _one_line_json({"event": "start", "request_id": request_id, "model": settings.QUERY_MODEL})
        output_tokens = 0
        
        try:
            async for response_token in retrieval_augmented_generation(body.query):

                if await request.is_disconnected():
                    break

                output_tokens += 1
                yield _one_line_json({"event": "token", "text": response_token})

            yield _one_line_json({"event": "end", "output_tokens": output_tokens, "latency_ms": int((time.perf_counter() - start_time) * 1000)})

        except asyncio.CancelledError:
            return
        except ValueError as e: #Do I really need this?
            yield _one_line_json({"event": "error", "type": "BadRequest", "error": str(e)})
            return
        except Exception as e: #This is a catch-all, not very informative.
            yield _one_line_json({"event": "error", "type": "InternalServerError", "error": str(e)})
            return

    headers = {
        "cache-control": "no-cache",
        "X-Accel-Buffering": "no",
    }

    return StreamingResponse(generate_ndjson_response(), media_type="application/x-ndjson", headers=headers)

        






