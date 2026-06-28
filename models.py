"""
ORM models — metadata only.

Large datasets (OHLCV, news, fundamentals, macro) are NEVER stored in PostgreSQL.
Only the metadata that describes where a dataset lives (storage_uri) is stored here.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from shared.db.session import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DatasetRecord(Base):
    """One row per successfully ingested dataset file."""

    __tablename__ = "dataset_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    data_type: Mapped[str] = mapped_column(String(32), nullable=False)   # ohlcv/news/fundamentals/macro
    symbol: Mapped[str | None] = mapped_column(String(32), nullable=True)
    timeframe: Mapped[str | None] = mapped_column(String(16), nullable=True)
    series: Mapped[str | None] = mapped_column(String(64), nullable=True)   # macro series name
    start_date: Mapped[str | None] = mapped_column(String(32), nullable=True)
    end_date: Mapped[str | None] = mapped_column(String(32), nullable=True)
    rows: Mapped[int | None] = mapped_column(Integer, nullable=True)
    columns: Mapped[int | None] = mapped_column(Integer, nullable=True)
    storage_uri: Mapped[str] = mapped_column(Text, nullable=False)
    hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    quality_passed: Mapped[bool] = mapped_column(Boolean, default=True)
    quality_issues: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False
    )

    logs: Mapped[list["IngestionLog"]] = relationship(
        "IngestionLog", back_populates="dataset", cascade="all, delete-orphan"
    )


class IngestionLog(Base):
    """Audit trail — one row per ingestion attempt (success or failure)."""

    __tablename__ = "ingestion_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dataset_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("dataset_records.id", ondelete="SET NULL"), nullable=True
    )
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    data_type: Mapped[str] = mapped_column(String(32), nullable=False)
    symbol: Mapped[str | None] = mapped_column(String(32), nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False)   # "success" | "failed" | "partial"
    rows_fetched: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rows_after_quality: Mapped[int | None] = mapped_column(Integer, nullable=True)
    issues: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    dataset: Mapped["DatasetRecord | None"] = relationship(
        "DatasetRecord", back_populates="logs"
    )
