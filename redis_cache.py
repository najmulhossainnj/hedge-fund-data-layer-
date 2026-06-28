"""
DatasetCache — hash-based Redis cache for dataset storage URIs.

Key pattern: ds:{sha256_hash}
Value:        s3://bucket/path/to/file.parquet

This is a pure cache — it stores nothing about the data itself, only the
mapping from a deterministic request hash to a Parquet file location.
"""

from __future__ import annotations

import logging
from typing import Optional

import redis.asyncio as aioredis

from shared.config import settings

logger = logging.getLogger(__name__)

_KEY_PREFIX = "ds:"


class DatasetCache:
    def __init__(self) -> None:
        self._client: Optional[aioredis.Redis] = None

    async def _get_client(self) -> aioredis.Redis:
        if self._client is None:
            self._client = aioredis.from_url(
                settings.REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=3,
            )
        return self._client

    async def get(self, hash_val: str) -> Optional[str]:
        """Return the cached storage_uri, or None on miss."""
        try:
            client = await self._get_client()
            val = await client.get(f"{_KEY_PREFIX}{hash_val}")
            if val:
                logger.debug("Cache HIT: hash=%s", hash_val[:12])
            else:
                logger.debug("Cache MISS: hash=%s", hash_val[:12])
            return val
        except Exception as exc:
            # Cache errors must never break the delivery path
            logger.warning("DatasetCache.get error: %s", exc)
            return None

    async def set(self, hash_val: str, uri: str, ttl: int) -> None:
        """Store hash → URI with a TTL in seconds."""
        try:
            client = await self._get_client()
            await client.setex(f"{_KEY_PREFIX}{hash_val}", ttl, uri)
            logger.debug("Cache SET: hash=%s ttl=%ds", hash_val[:12], ttl)
        except Exception as exc:
            logger.warning("DatasetCache.set error: %s", exc)

    async def invalidate(self, hash_val: str) -> None:
        """Explicit invalidation — used when upstream data is retroactively revised."""
        try:
            client = await self._get_client()
            deleted = await client.delete(f"{_KEY_PREFIX}{hash_val}")
            if deleted:
                logger.info("Cache INVALIDATED: hash=%s", hash_val[:12])
        except Exception as exc:
            logger.warning("DatasetCache.invalidate error: %s", exc)

    async def ping(self) -> bool:
        """Health check — True if Redis is reachable."""
        try:
            client = await self._get_client()
            return await client.ping()
        except Exception:
            return False

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None


# Module-level singleton used by the delivery layer
dataset_cache = DatasetCache()
