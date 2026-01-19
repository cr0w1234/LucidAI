from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader

import hmac
from rag_agent.core.config import settings

api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

def authenticate_api_key(provided_api_key: str = Depends(api_key_header)) -> None:
    expected_api_key = settings.BACKEND_API_KEY.get_secret_value()
    if not provided_api_key or not hmac.compare_digest(provided_api_key, expected_api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "X-API-Key realm='api'"},
        )