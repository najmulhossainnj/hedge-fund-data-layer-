"""
IngestionPipeline — orchestrates the full ingestion flow:

  fetch → normalize → validate → store Parquet → write metadata → cache → publish event

Called by:
  1. The delivery endpoints when no cached dataset exists (inline, synchronous).
  2. Future background workers / cron jobs for pre-warming datasets.

Step sequence (from spec):
  1.  Compute SHA-256 hash
  2.  Check Redis cache → if hit, return URI
  3.  Select provider from registry
  4.  provider.download_*() → raw Pandas DataFrame / dict
  5.  normalizer.normalize() → Polars DataFrame
  6.  validators.run_all_*() → log issues to PostgreSQL
  7.  Write Parquet to MinIO
  8.  Write DatasetRecord to PostgreSQL
  9.  Set Redis cache hash → URI with TTL
  10. Publish DatasetIngested to Redis Stream
  11. Return storage_uri
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import polars as pl


from ingestion.normalizers.fundamentals import FundamentalsNormalizer
from ingestion.normalizers.macro import MacroNormalizer
from ingestion.normalizers.news import NewsNormalizer
from ingestion.normalizers.ohlcv import OHLCVNormalizer
from ingestion.providers import registry as provider_registry
from ingestion.quality import validators
from ingestion.storage.parquet_store import ParquetStore
from ingestion.storage.registry import DatasetRegistry
from shared.config import settings
from shared.db.session import get_session
from shared.events.streams import (
    event_stream,
    publish_dataset_ingested,
    publish_ingestion_failed,
)

logger = logging.getLogger(__name__)

# ── Hash computation ──────────────────────────────────────────────────────────


def compute_hash(data_type: str, params: dict) -> str:
    """
    Deterministic SHA-256 hash used as the cache key and deduplication key.

    The hash is generated from every parameter that would produce a different
    dataset — provider, data_type, symbol, timeframe, date range, adjusted flag.
    """
    key_data = {"data_type": data_type, **{k: str(v) for k, v in sorted(params.items())}}
    serialized = json.dumps(key_data, sort_keys=True)
    return hashlib.sha256(serialized.encode()).hexdigest()


# ── TTL selector ──────────────────────────────────────────────────────────────


def _cache_ttl(data_type: str, timeframe: Optional[str] = None) -> int:
    if data_type == "ohlcv":
        intraday = timeframe and timeframe not in ("1d", "1w", "1m")
        return settings.CACHE_TTL_OHLCV_INTRADAY if intraday else settings.CACHE_TTL_OHLCV_DAILY
    if data_type == "news":
        return settings.CACHE_TTL_NEWS
    if data_type == "fundamentals":
        return settings.CACHE_TTL_FUNDAMENTALS
    if data_type == "macro":
        return settings.CACHE_TTL_MACRO
    return settings.CACHE_TTL_OHLCV_DAILY


# ── Pipeline ──────────────────────────────────────────────────────────────────


class IngestionPipeline:
    def __init__(self) -> None:
        self._parquet = ParquetStore()
        self._registry = DatasetRegistry()
        self._ohlcv_norm = OHLCVNormalizer()
        self._news_norm = NewsNormalizer()
        self._fund_norm = FundamentalsNormalizer()
        self._macro_norm = MacroNormalizer()

    # ── Cache helpers (direct Redis; no full DatasetCache import to avoid cycle) ─

    async def _cache_get(self, hash_val: str) -> Optional[str]:
        import redis.asyncio as aioredis
        client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        try:
            return await client.get(f"ds:{hash_val}")
        finally:
            await client.aclose()

    async def _cache_set(self, hash_val: str, uri: str, ttl: int) -> None:
        import redis.asyncio as aioredis
        client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        try:
            await client.setex(f"ds:{hash_val}", ttl, uri)
        finally:
            await client.aclose()

    # ── OHLCV ─────────────────────────────────────────────────────────────

    async def run_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: str,
        end: str,
        provider_name: str = "yahoo",
    ) -> str:
        """Return storage_uri for an OHLCV dataset, ingesting if not cached."""
        params = {
            "provider": provider_name,
            "symbol": symbol,
            "timeframe": timeframe,
            "start": start,
            "end": end,
        }
        hash_val = compute_hash("ohlcv", params)

        # ── Step 2: cache check ───────────────────────────────────────────
        cached_uri = await self._cache_get(hash_val)
        if cached_uri:
            logger.info("IngestionPipeline: cache hit hash=%s", hash_val[:12])
            return cached_uri

        provider = provider_registry.get(provider_name)

        async with get_session() as session:
            # ── Check DB registry (survived Redis flush) ──────────────────
            existing = await self._registry.get_by_hash(session, hash_val)
            if existing:
                await self._cache_set(hash_val, existing.storage_uri, _cache_ttl("ohlcv", timeframe))
                return existing.storage_uri

            log = await self._registry.log_start(
                session, provider=provider_name, data_type="ohlcv", symbol=symbol
            )

            try:
                # ── Step 4: download ──────────────────────────────────────
                raw_df = await provider.download_ohlcv(symbol, timeframe, start, end)
                rows_fetched = len(raw_df)

                # ── Step 5: normalize ─────────────────────────────────────
                norm_df = self._ohlcv_norm.normalize(
                    raw_df, symbol=symbol, timeframe=timeframe, provider=provider_name
                )

                # ── Step 6: validate ──────────────────────────────────────
                quality_passed, quality_issues = validators.run_all_ohlcv(norm_df, symbol)
                rows_after = len(norm_df)

                # ── Step 7: write Parquet ─────────────────────────────────
                date_tag = start.replace("-", "")
                uri = await self._parquet.write(
                    norm_df,
                    "ohlcv",
                    symbol=symbol,
                    timeframe=timeframe,
                    date_tag=date_tag,
                    hash_suffix=hash_val,
                )

                # ── Step 8: write metadata ────────────────────────────────
                record = await self._registry.create(
                    session,
                    provider=provider_name,
                    data_type="ohlcv",
                    storage_uri=uri,
                    hash_val=hash_val,
                    symbol=symbol,
                    timeframe=timeframe,
                    start_date=start,
                    end_date=end,
                    rows=rows_after,
                    columns=len(norm_df.columns),
                    quality_passed=quality_passed,
                    quality_issues=quality_issues,
                )

                await self._registry.log_success(
                    session, log,
                    dataset_id=record.id,
                    rows_fetched=rows_fetched,
                    rows_after_quality=rows_after,
                    issues=quality_issues,
                )

                # ── Step 9: set cache ──────────────────────────────────────
                ttl = _cache_ttl("ohlcv", timeframe)
                await self._cache_set(hash_val, uri, ttl)

                # ── Step 10: publish event ────────────────────────────────
                await publish_dataset_ingested(
                    event_stream,
                    data_type="ohlcv",
                    symbol=symbol,
                    timeframe=timeframe,
                    storage_uri=uri,
                    hash_val=hash_val,
                    rows=rows_after,
                )

                logger.info(
                    "IngestionPipeline.run_ohlcv: %s %s rows=%d uri=%s",
                    symbol, timeframe, rows_after, uri,
                )
                return uri

            except Exception as exc:
                await self._registry.log_failure(session, log, error=str(exc))
                await publish_ingestion_failed(
                    event_stream, data_type="ohlcv", symbol=symbol, error=str(exc)
                )
                raise

    # ── News ──────────────────────────────────────────────────────────────

    async def run_news(
        self,
        symbol: str,
        start: str,
        end: str,
        provider_name: str = "news",
    ) -> str:
        params = {"provider": provider_name, "symbol": symbol, "start": start, "end": end}
        hash_val = compute_hash("news", params)

        cached_uri = await self._cache_get(hash_val)
        if cached_uri:
            return cached_uri

        provider = provider_registry.get(provider_name)

        async with get_session() as session:
            existing = await self._registry.get_by_hash(session, hash_val)
            if existing:
                await self._cache_set(hash_val, existing.storage_uri, settings.CACHE_TTL_NEWS)
                return existing.storage_uri

            log = await self._registry.log_start(
                session, provider=provider_name, data_type="news", symbol=symbol
            )
            try:
                raw_df = await provider.download_news(symbol, start, end)
                rows_fetched = len(raw_df)

                norm_df = self._news_norm.normalize(
                    raw_df, symbol=symbol, provider=provider_name
                )

                quality_passed, quality_issues = validators.run_all_news(norm_df)
                rows_after = len(norm_df)

                date_tag = start.replace("-", "")
                uri = await self._parquet.write(
                    norm_df, "news",
                    symbol=symbol, date_tag=date_tag, hash_suffix=hash_val,
                )

                record = await self._registry.create(
                    session,
                    provider=provider_name, data_type="news",
                    storage_uri=uri, hash_val=hash_val,
                    symbol=symbol, start_date=start, end_date=end,
                    rows=rows_after, columns=len(norm_df.columns),
                    quality_passed=quality_passed, quality_issues=quality_issues,
                )

                await self._registry.log_success(
                    session, log, dataset_id=record.id,
                    rows_fetched=rows_fetched, rows_after_quality=rows_after,
                    issues=quality_issues,
                )

                await self._cache_set(hash_val, uri, settings.CACHE_TTL_NEWS)
                await publish_dataset_ingested(
                    event_stream, data_type="news", symbol=symbol,
                    timeframe=None, storage_uri=uri, hash_val=hash_val, rows=rows_after,
                )
                return uri

            except Exception as exc:
                await self._registry.log_failure(session, log, error=str(exc))
                await publish_ingestion_failed(
                    event_stream, data_type="news", symbol=symbol, error=str(exc)
                )
                raise

    # ── Fundamentals ──────────────────────────────────────────────────────

    async def run_fundamentals(
        self,
        symbol: str,
        provider_name: str = "yahoo",
    ) -> str:
        params = {"provider": provider_name, "symbol": symbol}
        hash_val = compute_hash("fundamentals", params)

        cached_uri = await self._cache_get(hash_val)
        if cached_uri:
            return cached_uri

        provider = provider_registry.get(provider_name)

        async with get_session() as session:
            existing = await self._registry.get_by_hash(session, hash_val)
            if existing:
                await self._cache_set(hash_val, existing.storage_uri, settings.CACHE_TTL_FUNDAMENTALS)
                return existing.storage_uri

            log = await self._registry.log_start(
                session, provider=provider_name, data_type="fundamentals", symbol=symbol
            )
            try:
                raw_dict = await provider.download_fundamentals(symbol)

                norm_df = self._fund_norm.normalize(
                    raw_dict, symbol=symbol, provider=provider_name
                )

                date_tag = datetime.now(timezone.utc).strftime("%Y%m%d")
                uri = await self._parquet.write(
                    norm_df, "fundamentals",
                    symbol=symbol, date_tag=date_tag, hash_suffix=hash_val,
                )

                record = await self._registry.create(
                    session,
                    provider=provider_name, data_type="fundamentals",
                    storage_uri=uri, hash_val=hash_val,
                    symbol=symbol, rows=len(norm_df), columns=len(norm_df.columns),
                )

                await self._registry.log_success(
                    session, log, dataset_id=record.id,
                    rows_fetched=1, rows_after_quality=1,
                )

                await self._cache_set(hash_val, uri, settings.CACHE_TTL_FUNDAMENTALS)
                await publish_dataset_ingested(
                    event_stream, data_type="fundamentals", symbol=symbol,
                    timeframe=None, storage_uri=uri, hash_val=hash_val, rows=1,
                )
                return uri

            except Exception as exc:
                await self._registry.log_failure(session, log, error=str(exc))
                raise

    # ── Macro ─────────────────────────────────────────────────────────────

    async def run_macro(
        self,
        series: str,
        start: str,
        end: str,
        provider_name: str = "fred",
    ) -> str:
        params = {"provider": provider_name, "series": series, "start": start, "end": end}
        hash_val = compute_hash("macro", params)

        cached_uri = await self._cache_get(hash_val)
        if cached_uri:
            return cached_uri

        provider = provider_registry.get(provider_name)

        async with get_session() as session:
            existing = await self._registry.get_by_hash(session, hash_val)
            if existing:
                await self._cache_set(hash_val, existing.storage_uri, settings.CACHE_TTL_MACRO)
                return existing.storage_uri

            log = await self._registry.log_start(
                session, provider=provider_name, data_type="macro"
            )
            try:
                raw_df = await provider.download_macro(series, start, end)
                rows_fetched = len(raw_df)

                norm_df = self._macro_norm.normalize(
                    raw_df, series=series, provider=provider_name
                )

                year = start[:4]
                uri = await self._parquet.write(
                    norm_df, "macro",
                    series=series, date_tag=f"{year}0101", hash_suffix=hash_val,
                )

                record = await self._registry.create(
                    session,
                    provider=provider_name, data_type="macro",
                    storage_uri=uri, hash_val=hash_val,
                    series=series, start_date=start, end_date=end,
                    rows=len(norm_df), columns=len(norm_df.columns),
                )

                await self._registry.log_success(
                    session, log, dataset_id=record.id,
                    rows_fetched=rows_fetched, rows_after_quality=len(norm_df),
                )

                await self._cache_set(hash_val, uri, settings.CACHE_TTL_MACRO)
                await publish_dataset_ingested(
                    event_stream, data_type="macro", symbol=None,
                    timeframe=None, storage_uri=uri, hash_val=hash_val, rows=len(norm_df),
                )
                return uri

            except Exception as exc:
                await self._registry.log_failure(session, log, error=str(exc))
                raise
