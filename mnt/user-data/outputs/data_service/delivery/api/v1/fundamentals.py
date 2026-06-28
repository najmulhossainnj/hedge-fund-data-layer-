"""
GET /api/v1/fundamentals

Research Layer contract (do not change):
  Request:  ?symbol=AAPL
  Response: Single JSON OBJECT (not an array):
    {symbol, pe_ratio, pb_ratio, revenue_growth, earnings_surprise,
     market_cap, eps, as_of}
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status

from delivery.cache.redis_cache import dataset_cache
from delivery.query.duckdb_reader import duckdb_reader
from ingestion.pipeline import IngestionPipeline, compute_hash
from shared.auth.dependencies import verify_api_key
from shared.events.streams import event_stream, publish_cache_hit

logger = logging.getLogger(__name__)

router = APIRouter()
_pipeline = IngestionPipeline()


@router.get(
    "/fundamentals",
    summary="Get fundamental metrics",
    response_description="Single JSON object with latest fundamental metrics",
    dependencies=[Depends(verify_api_key)],
)
async def get_fundamentals(
    symbol: str = Query(..., description="Ticker symbol, e.g. AAPL"),
) -> dict[str, Any]:
    """
    Returns the latest fundamental metrics for the requested symbol.

    Returns a single object (not an array).
    Fields are null when the provider does not supply them.
    """
    t_start = time.perf_counter()
    symbol = symbol.upper()

    params = {"provider": "yahoo", "symbol": symbol}
    hash_val = compute_hash("fundamentals", params)

    uri = await dataset_cache.get(hash_val)
    if uri:
        await publish_cache_hit(event_stream, hash_val=hash_val, symbol=symbol)
    else:
        try:
            uri = await _pipeline.run_fundamentals(symbol=symbol)
        except Exception as exc:
            logger.error("Fundamentals ingestion failed for %s: %s", symbol, exc)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Upstream provider unavailable. Try again shortly.",
            )

    try:
        row = await duckdb_reader.read_fundamentals(uri)
    except Exception as exc:
        logger.error("DuckDB read failed for fundamentals %s: %s", uri, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to read fundamentals from storage.",
        )

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No fundamentals data found for {symbol}.",
        )

    elapsed = time.perf_counter() - t_start
    logger.info("GET /fundamentals symbol=%s elapsed=%.3fs", symbol, elapsed)
    return row
