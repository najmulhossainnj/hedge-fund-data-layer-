"""
GET /api/v1/health  — liveness and readiness probe.

Checks connectivity to all three infrastructure dependencies:
  postgres  — SQLAlchemy async ping
  redis     — PING command
  minio     — HeadBucket request
"""

from __future__ import annotations

import logging

from fastapi import APIRouter
from sqlalchemy import text

from delivery.cache.redis_cache import dataset_cache
from ingestion.storage.parquet_store import ParquetStore, _s3_client
from shared.db.session import AsyncSessionLocal
from shared.models.responses import HealthResponse

logger = logging.getLogger(__name__)

router = APIRouter()
_parquet = ParquetStore()


async def _check_postgres() -> str:
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        return "ok"
    except Exception as exc:
        logger.warning("Health: postgres check failed: %s", exc)
        return f"error: {exc}"


async def _check_redis() -> str:
    ok = await dataset_cache.ping()
    return "ok" if ok else "error: unreachable"


async def _check_minio() -> str:
    import asyncio
    from shared.config import settings

    def _sync() -> str:
        try:
            client = _s3_client()
            client.head_bucket(Bucket=settings.MINIO_BUCKET)
            return "ok"
        except Exception as exc:
            return f"error: {exc}"

    return await asyncio.to_thread(_sync)


@router.get(
    "/health",
    summary="Service health check",
    response_model=HealthResponse,
    tags=["management"],
)
async def health_check() -> HealthResponse:
    postgres = await _check_postgres()
    redis = await _check_redis()
    minio = await _check_minio()

    all_ok = all(s == "ok" for s in (postgres, redis, minio))
    degraded = not all_ok and any(s == "ok" for s in (postgres, redis, minio))

    return HealthResponse(
        status="healthy" if all_ok else ("degraded" if degraded else "unhealthy"),
        postgres=postgres,
        redis=redis,
        minio=minio,
    )
