"""
Response models for the Data Service API.

IMPORTANT:
- Data endpoints (/ohlcv, /news, /fundamentals, /macro) return RAW arrays / objects
  with NO envelope. The Research Layer expects exactly that shape.
- Management / admin endpoints use APIResponse for consistency.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel


# ── Management envelope (admin/health endpoints only) ─────────────────────────


class APIResponse(BaseModel):
    status: str              # "success" | "error"
    message: str
    data: Any = None
    metadata: dict = {}
    execution_time: float = 0.0


# ── Data endpoint response models ────────────────────────────────────────────
# These match EXACTLY what the Research Layer's market_data_client.py expects.


class OHLCVBar(BaseModel):
    """One row in the /ohlcv response array."""
    timestamp: str           # ISO 8601, no timezone suffix — pd.to_datetime() handles it
    open: float
    high: float
    low: float
    close: float
    volume: float


class NewsRow(BaseModel):
    """One row in the /news response array."""
    headline: str
    published_at: str        # ISO 8601 with timezone — pd.to_datetime(..., utc=True)
    source: Optional[str] = None
    url: Optional[str] = None
    symbol: Optional[str] = None


class FundamentalsResponse(BaseModel):
    """The /fundamentals response — single object, not an array."""
    symbol: str
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    revenue_growth: Optional[float] = None
    earnings_surprise: Optional[float] = None
    market_cap: Optional[float] = None
    eps: Optional[float] = None
    as_of: str               # ISO date string


class MacroRow(BaseModel):
    """One row in the /macro response array."""
    date: str                # ISO date string
    series: str
    value: float


# ── Health ────────────────────────────────────────────────────────────────────


class HealthResponse(BaseModel):
    status: str              # "healthy" | "degraded" | "unhealthy"
    postgres: str
    redis: str
    minio: str
    version: str = "1.0.0"
