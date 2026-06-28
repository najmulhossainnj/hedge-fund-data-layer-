"""
GET /api/v1/news

Research Layer contract (do not change):
  Request:  ?symbol=AAPL&start=2023-01-01&end=2024-01-01
  Response: JSON array of {headline, published_at, source, url, symbol}
            or [] if no news.

published_at MUST include a timezone suffix (Z) — the Research Layer calls
pd.to_datetime(..., utc=True) on it.
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


@router.get(
    "/news",
    summary="Get news articles",
    response_description="Array of news articles with sentiment-ready headlines",
    dependencies=[Depends(verify_api_key)],
)
async def get_news(
    symbol: str = Query(..., description="Ticker symbol, e.g. AAPL"),
    start: str = Query(..., description="Start date ISO 8601, e.g. 2023-01-01"),
    end: str = Query(..., description="End date ISO 8601, e.g. 2024-01-01"),
) -> list[dict[str, Any]]:
    """
    Returns news articles for the requested symbol and date range.

    The Research Layer's FinBERT pipeline requires:
      - headline  (str)
      - published_at (ISO 8601 with timezone)
    """
    t_start = time.perf_counter()
    symbol = symbol.upper()

    params = {"provider": "news", "symbol": symbol, "start": start, "end": end}
    hash_val = compute_hash("news", params)

    uri = await dataset_cache.get(hash_val)
    if uri:
        await publish_cache_hit(event_stream, hash_val=hash_val, symbol=symbol)
    else:
        try:
            uri = await _pipeline.run_news(symbol=symbol, start=start, end=end)
        except Exception as exc:
            logger.error("News ingestion failed for %s: %s", symbol, exc)
            # Return empty array — the Research Layer handles this gracefully
            return []

    try:
        rows = await duckdb_reader.read_news(uri, start=start, end=end)
    except Exception as exc:
        logger.error("DuckDB read failed for news %s: %s", uri, exc)
        return []

    elapsed = time.perf_counter() - t_start
    logger.info(
        "GET /news symbol=%s articles=%d elapsed=%.3fs",
        symbol, len(rows), elapsed,
    )
    return rows
