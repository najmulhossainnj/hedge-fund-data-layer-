"""
GET /api/v1/ohlcv

Research Layer contract (do not change):
  Request:  ?symbol=AAPL&timeframe=1d&start=2023-01-01&end=2024-01-01
  Response: JSON array of {timestamp, open, high, low, close, volume}
            or [] if no data.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from delivery.cache.redis_cache import dataset_cache
from delivery.query.duckdb_reader import duckdb_reader
from ingestion.pipeline import IngestionPipeline, compute_hash
from shared.auth.dependencies import verify_api_key
from shared.config import settings
from shared.events.streams import event_stream, publish_cache_hit

logger = logging.getLogger(__name__)

router = APIRouter()
_pipeline = IngestionPipeline()


@router.get(
    "/ohlcv",
    summary="Get OHLCV bars",
    response_description="Array of OHLCV bars ordered oldest → newest",
    dependencies=[Depends(verify_api_key)],
)
async def get_ohlcv(
    symbol: str = Query(..., description="Ticker symbol, e.g. AAPL"),
    timeframe: str = Query("1d", description="Bar timeframe: 1d, 1w, 1h, 1m"),
    start: str = Query(..., description="Start date ISO 8601, e.g. 2023-01-01"),
    end: str = Query(..., description="End date ISO 8601, e.g. 2024-01-01"),
) -> list[dict[str, Any]]:
    """
    Returns OHLCV bars for the requested symbol and date range.

    Flow:
      1. Compute deterministic hash from params
      2. Check Redis cache for a stored Parquet URI
      3. If miss, check PostgreSQL registry (survived Redis flush)
      4. If still miss, trigger ingestion pipeline inline
      5. Query Parquet via DuckDB with predicate pushdown
      6. Return list of {timestamp, open, high, low, close, volume}
    """
    t_start = time.perf_counter()
    symbol = symbol.upper()

    params = {
        "provider": "yahoo",
        "symbol": symbol,
        "timeframe": timeframe,
        "start": start,
        "end": end,
    }
    hash_val = compute_hash("ohlcv", params)

    # ── Step 1-2: Redis cache ─────────────────────────────────────────────
    uri = await dataset_cache.get(hash_val)
    if uri:
        await publish_cache_hit(event_stream, hash_val=hash_val, symbol=symbol)
    else:
        # ── Step 3-4: ingest (checks DB registry internally) ─────────────
        try:
            uri = await _pipeline.run_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                start=start,
                end=end,
            )
        except Exception as exc:
            logger.error("OHLCV ingestion failed for %s: %s", symbol, exc)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Upstream provider unavailable. Try again shortly.",
            )

    # ── Step 5: DuckDB query ──────────────────────────────────────────────
    try:
        rows = await duckdb_reader.read_ohlcv(uri, start=start, end=end)
    except Exception as exc:
        logger.error("DuckDB read failed for %s: %s", uri, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to read dataset from storage.",
        )

    elapsed = time.perf_counter() - t_start
    logger.info(
        "GET /ohlcv symbol=%s timeframe=%s rows=%d elapsed=%.3fs",
        symbol, timeframe, len(rows), elapsed,
    )

    # ── Step 6: return raw array (NO envelope) ────────────────────────────
    return rows
