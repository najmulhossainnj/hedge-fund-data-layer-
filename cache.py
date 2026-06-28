"""
Cache management endpoints.

GET  /api/v1/cache/stats              — cache key counts, memory usage
POST /api/v1/cache/refresh            — force re-ingestion by invalidating a hash
DELETE /api/v1/cache/invalidate       — remove a specific hash from cache
DELETE /api/v1/cache/flush/{pattern}  — flush all keys matching a pattern

These are management endpoints — they use the APIResponse envelope and
require the same API key as the data endpoints.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query, status

from delivery.cache.redis_cache import dataset_cache
from ingestion.pipeline import compute_hash
from shared.auth.dependencies import verify_api_key
from shared.config import settings
from shared.models.responses import APIResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/cache", dependencies=[Depends(verify_api_key)])


async def _raw_client() -> aioredis.Redis:
    return aioredis.from_url(settings.REDIS_URL, decode_responses=True)


# ── Stats ─────────────────────────────────────────────────────────────────────


@router.get("/stats", response_model=APIResponse, summary="Cache statistics")
async def cache_stats() -> APIResponse:
    """
    Returns the number of cached dataset keys and Redis memory usage.
    """
    t = time.perf_counter()
    try:
        client = await _raw_client()
        keys = await client.keys("ds:*")
        info = await client.info("memory")
        await client.aclose()

        return APIResponse(
            status="success",
            message="Cache statistics retrieved.",
            data={
                "cached_datasets": len(keys),
                "used_memory_human": info.get("used_memory_human"),
                "used_memory_bytes": info.get("used_memory"),
            },
            execution_time=round(time.perf_counter() - t, 4),
        )
    except Exception as exc:
        logger.error("cache_stats failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Redis unavailable: {exc}",
        )


# ── Refresh (force re-ingestion) ──────────────────────────────────────────────


@router.post("/refresh", response_model=APIResponse, summary="Force re-ingestion")
async def cache_refresh(
    data_type: str = Query(..., description="ohlcv | news | fundamentals | macro"),
    symbol: str | None = Query(None),
    timeframe: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
    series: str | None = Query(None),
    provider: str = Query("yahoo"),
) -> APIResponse:
    """
    Invalidates the cache for a specific dataset so the next request
    triggers a fresh download from the upstream provider.

    Use this when:
    - Corporate action adjustments have been revised retroactively.
    - Provider data has been corrected.
    - You want to force an update before the TTL expires.
    """
    t = time.perf_counter()

    params: dict[str, Any] = {"provider": provider}
    if symbol:
        params["symbol"] = symbol.upper()
    if timeframe:
        params["timeframe"] = timeframe
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if series:
        params["series"] = series.upper()

    hash_val = compute_hash(data_type, params)
    await dataset_cache.invalidate(hash_val)

    logger.info("Cache refresh: data_type=%s hash=%s", data_type, hash_val[:12])

    return APIResponse(
        status="success",
        message=f"Cache invalidated for {data_type}. Next request will re-ingest.",
        data={"hash": hash_val, "data_type": data_type, "params": params},
        execution_time=round(time.perf_counter() - t, 4),
    )


# ── Invalidate by hash ────────────────────────────────────────────────────────


@router.delete("/invalidate", response_model=APIResponse, summary="Invalidate by hash")
async def cache_invalidate(
    hash_val: str = Query(..., description="SHA-256 hash of the dataset to invalidate"),
) -> APIResponse:
    """
    Directly invalidates a cache entry by its SHA-256 hash.
    Use /cache/stats to discover active hashes.
    """
    t = time.perf_counter()
    await dataset_cache.invalidate(hash_val)

    return APIResponse(
        status="success",
        message=f"Cache key invalidated: {hash_val[:12]}...",
        data={"hash": hash_val},
        execution_time=round(time.perf_counter() - t, 4),
    )


# ── Flush by symbol (bulk invalidation) ──────────────────────────────────────


@router.delete("/flush", response_model=APIResponse, summary="Flush all cache for a symbol")
async def cache_flush_symbol(
    symbol: str = Query(..., description="Symbol to flush all cached datasets for"),
) -> APIResponse:
    """
    Flushes ALL cached datasets for a given symbol (ohlcv, news, fundamentals).

    Useful after a ticker rename, merger, or data correction.
    This is a scan-then-delete operation — avoid on very large caches.
    """
    t = time.perf_counter()
    symbol = symbol.upper()

    try:
        client = await _raw_client()

        # Scan all ds: keys and delete those whose stored value contains the symbol path
        all_keys = await client.keys("ds:*")
        deleted = 0

        # We can't know which hashes belong to a symbol without decoding the URI,
        # so we store a secondary index: sym:{symbol} -> set of hashes
        # For now, invalidate via the secondary index if it exists, otherwise scan values.
        sym_index_key = f"sym:{symbol}"
        hashes = await client.smembers(sym_index_key)

        if hashes:
            for h in hashes:
                await client.delete(f"ds:{h}")
                deleted += 1
            await client.delete(sym_index_key)
        else:
            # Fallback: linear scan (acceptable for maintenance ops)
            for key in all_keys:
                val = await client.get(key)
                if val and f"/{symbol}/" in val:
                    await client.delete(key)
                    deleted += 1

        await client.aclose()
        logger.info("Cache flush: symbol=%s deleted=%d", symbol, deleted)

        return APIResponse(
            status="success",
            message=f"Flushed {deleted} cache entries for {symbol}.",
            data={"symbol": symbol, "deleted": deleted},
            execution_time=round(time.perf_counter() - t, 4),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Redis unavailable: {exc}",
        )
