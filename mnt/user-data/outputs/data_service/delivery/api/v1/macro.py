"""
GET /api/v1/macro

Research Layer contract (do not change):
  Request:  ?series=CPI&start=2023-01-01&end=2024-01-01
  Response: JSON array of {date, series, value}

Supported series: CPI, FED_FUNDS_RATE, GDP_GROWTH, UNEMPLOYMENT
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
from shared.events.streams import event_stream, publish_cache_hit

logger = logging.getLogger(__name__)

router = APIRouter()
_pipeline = IngestionPipeline()

_SUPPORTED_SERIES = {"CPI", "FED_FUNDS_RATE", "GDP_GROWTH", "UNEMPLOYMENT"}


@router.get(
    "/macro",
    summary="Get macro time series",
    response_description="Array of macro data points",
    dependencies=[Depends(verify_api_key)],
)
async def get_macro(
    series: str = Query(..., description=f"Series name. One of: {', '.join(sorted(_SUPPORTED_SERIES))}"),
    start: str = Query(..., description="Start date ISO 8601, e.g. 2023-01-01"),
    end: str = Query(..., description="End date ISO 8601, e.g. 2024-01-01"),
) -> list[dict[str, Any]]:
    """
    Returns a macro time series for the requested date range.

    Uses FRED as the data source (free API key required: fred.stlouisfed.org).
    Returns [] if no data is available for the range.
    """
    t_start = time.perf_counter()
    series = series.upper()

    if series not in _SUPPORTED_SERIES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported series '{series}'. Supported: {sorted(_SUPPORTED_SERIES)}",
        )

    params = {"provider": "fred", "series": series, "start": start, "end": end}
    hash_val = compute_hash("macro", params)

    uri = await dataset_cache.get(hash_val)
    if uri:
        await publish_cache_hit(event_stream, hash_val=hash_val, symbol=None)
    else:
        try:
            uri = await _pipeline.run_macro(series=series, start=start, end=end)
        except Exception as exc:
            logger.error("Macro ingestion failed for %s: %s", series, exc)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Upstream provider unavailable. Try again shortly.",
            )

    try:
        rows = await duckdb_reader.read_macro(uri, series=series, start=start, end=end)
    except Exception as exc:
        logger.error("DuckDB read failed for macro %s: %s", uri, exc)
        return []

    elapsed = time.perf_counter() - t_start
    logger.info(
        "GET /macro series=%s rows=%d elapsed=%.3fs", series, len(rows), elapsed
    )
    return rows
