"""
Event bus backed by Redis Streams.

Events published:
  DatasetIngested       — a new dataset was written to object storage
  DatasetServedFromCache — a delivery request was fulfilled from Redis cache
  IngestionFailed       — an ingestion pipeline run failed

Stream name: data_service.events
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis

from shared.config import settings

logger = logging.getLogger(__name__)

STREAM_NAME = "data_service.events"


class EventStream:
    def __init__(self) -> None:
        self._client: aioredis.Redis | None = None

    async def _get_client(self) -> aioredis.Redis:
        if self._client is None:
            self._client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        return self._client

    async def publish(self, event: str, payload: dict[str, Any]) -> None:
        """Append an event to the Redis Stream. Non-blocking — never raises."""
        try:
            client = await self._get_client()
            fields: dict[str, str] = {
                "event": event,
                "payload": json.dumps(payload),
                "published_at": datetime.now(timezone.utc).isoformat(),
            }
            await client.xadd(STREAM_NAME, fields)
        except Exception as exc:
            # Event publishing must never crash the main pipeline
            logger.warning("EventStream.publish failed: %s", exc)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None


# ── Pre-built event helpers ───────────────────────────────────────────────────


async def publish_dataset_ingested(
    stream: EventStream,
    *,
    data_type: str,
    symbol: str | None,
    timeframe: str | None,
    storage_uri: str,
    hash_val: str,
    rows: int,
) -> None:
    await stream.publish(
        "DatasetIngested",
        {
            "data_type": data_type,
            "symbol": symbol,
            "timeframe": timeframe,
            "storage_uri": storage_uri,
            "hash": hash_val,
            "rows": rows,
        },
    )


async def publish_cache_hit(
    stream: EventStream, *, hash_val: str, symbol: str | None
) -> None:
    await stream.publish(
        "DatasetServedFromCache",
        {"hash": hash_val, "symbol": symbol},
    )


async def publish_ingestion_failed(
    stream: EventStream,
    *,
    data_type: str,
    symbol: str | None,
    error: str,
) -> None:
    await stream.publish(
        "IngestionFailed",
        {"data_type": data_type, "symbol": symbol, "error": error},
    )


# ── Module-level singleton ────────────────────────────────────────────────────

event_stream = EventStream()
