"""
Canonical data models — the single source of truth for every dataset in the system.

All providers normalize their raw output into these models.
The delivery layer serialises these into the exact JSON shapes expected by the Research Layer.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, field_validator, model_validator


# ── OHLCV / Candle ───────────────────────────────────────────────────────────


class Candle(BaseModel):
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    adjusted_close: Optional[float] = None
    symbol: str
    timeframe: str
    provider: str

    @model_validator(mode="after")
    def prices_are_positive(self) -> "Candle":
        for field in ("open", "high", "low", "close", "volume"):
            val = getattr(self, field)
            if val < 0:
                raise ValueError(f"{field} must be non-negative, got {val}")
        return self

    @model_validator(mode="after")
    def high_gte_low(self) -> "Candle":
        if self.high < self.low:
            raise ValueError(f"high ({self.high}) must be >= low ({self.low})")
        return self


# ── News ─────────────────────────────────────────────────────────────────────


class NewsItem(BaseModel):
    headline: str
    # Always timezone-aware UTC — the Research Layer calls pd.to_datetime(..., utc=True)
    published_at: datetime
    source: Optional[str] = None
    url: Optional[str] = None
    symbol: str
    provider: str

    @field_validator("headline")
    @classmethod
    def headline_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("headline must not be empty")
        return v.strip()


# ── Fundamentals ─────────────────────────────────────────────────────────────


class Fundamental(BaseModel):
    symbol: str
    as_of: datetime
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    revenue_growth: Optional[float] = None
    earnings_surprise: Optional[float] = None
    market_cap: Optional[float] = None
    eps: Optional[float] = None
    provider: str


# ── Macro ─────────────────────────────────────────────────────────────────────


class MacroPoint(BaseModel):
    date: datetime
    # Standardised series identifier used throughout the system
    # e.g. "CPI", "FED_FUNDS_RATE", "GDP_GROWTH", "UNEMPLOYMENT"
    series: str
    value: float
    provider: str


# ── Dataset metadata (stored in PostgreSQL, not Parquet) ─────────────────────


class DatasetMeta(BaseModel):
    provider: str
    data_type: str           # "ohlcv" | "news" | "fundamentals" | "macro"
    symbol: Optional[str] = None
    timeframe: Optional[str] = None
    series: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    adjusted: bool = True
