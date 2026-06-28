"""
API key authentication dependency.

Every data endpoint requires the X-API-Key header to match DATA_SERVICE_API_KEY.
The Research Layer's market_data_client.py must send this header on every request.
"""

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from shared.config import settings

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)


async def verify_api_key(api_key: str = Security(_api_key_header)) -> str:
    """
    FastAPI dependency — raises 401 if the key is missing or wrong.

    Usage:
        @router.get("/ohlcv", dependencies=[Depends(verify_api_key)])
        async def get_ohlcv(...): ...
    """
    if api_key != settings.DATA_SERVICE_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
        )
    return api_key
