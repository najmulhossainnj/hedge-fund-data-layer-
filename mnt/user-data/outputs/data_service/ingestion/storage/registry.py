"""
DatasetRegistry — reads and writes dataset metadata in PostgreSQL.

Only metadata is stored here (storage_uri, hash, row counts, quality flags).
The actual data lives in Parquet files on MinIO.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db.models import DatasetRecord, IngestionLog

logger = logging.getLogger(__name__)


class DatasetRegistry:
    # ── DatasetRecord ─────────────────────────────────────────────────────

    async def create(
        self,
        session: AsyncSession,
        *,
        provider: str,
        data_type: str,
        storage_uri: str,
        hash_val: str,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        series: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        rows: Optional[int] = None,
        columns: Optional[int] = None,
        quality_passed: bool = True,
        quality_issues: Optional[list[str]] = None,
    ) -> DatasetRecord:
        record = DatasetRecord(
            provider=provider,
            data_type=data_type,
            symbol=symbol,
            timeframe=timeframe,
            series=series,
            start_date=start_date,
            end_date=end_date,
            rows=rows,
            columns=columns,
            storage_uri=storage_uri,
            hash=hash_val,
            quality_passed=quality_passed,
            quality_issues={"issues": quality_issues} if quality_issues else None,
            created_at=datetime.now(timezone.utc),
        )
        session.add(record)
        await session.flush()
        logger.info(
            "DatasetRegistry: created record id=%d hash=%s uri=%s",
            record.id, hash_val[:12], storage_uri,
        )
        return record

    async def get_by_hash(
        self, session: AsyncSession, hash_val: str
    ) -> Optional[DatasetRecord]:
        stmt = select(DatasetRecord).where(DatasetRecord.hash == hash_val)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_by_symbol(
        self, session: AsyncSession, symbol: str, data_type: Optional[str] = None
    ) -> list[DatasetRecord]:
        stmt = select(DatasetRecord).where(DatasetRecord.symbol == symbol)
        if data_type:
            stmt = stmt.where(DatasetRecord.data_type == data_type)
        stmt = stmt.order_by(DatasetRecord.created_at.desc())
        result = await session.execute(stmt)
        return list(result.scalars().all())

    # ── IngestionLog ──────────────────────────────────────────────────────

    async def log_start(
        self,
        session: AsyncSession,
        *,
        provider: str,
        data_type: str,
        symbol: Optional[str] = None,
    ) -> IngestionLog:
        log = IngestionLog(
            provider=provider,
            data_type=data_type,
            symbol=symbol,
            started_at=datetime.now(timezone.utc),
            status="running",
        )
        session.add(log)
        await session.flush()
        return log

    async def log_success(
        self,
        session: AsyncSession,
        log: IngestionLog,
        *,
        dataset_id: int,
        rows_fetched: int,
        rows_after_quality: int,
        issues: Optional[list[str]] = None,
    ) -> None:
        log.dataset_id = dataset_id
        log.completed_at = datetime.now(timezone.utc)
        log.status = "success"
        log.rows_fetched = rows_fetched
        log.rows_after_quality = rows_after_quality
        log.issues = {"issues": issues} if issues else None
        await session.flush()

    async def log_failure(
        self,
        session: AsyncSession,
        log: IngestionLog,
        *,
        error: str,
    ) -> None:
        log.completed_at = datetime.now(timezone.utc)
        log.status = "failed"
        log.error = error
        await session.flush()
