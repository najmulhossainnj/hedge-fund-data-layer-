"""
Shared test fixtures — canonical sample data used across unit and integration tests.

Rules:
- Never use real provider APIs in any test.
- All fixtures are deterministic (fixed dates, fixed values).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import polars as pl


# ── OHLCV ─────────────────────────────────────────────────────────────────────


def make_raw_ohlcv_pandas(n: int = 5, symbol: str = "AAPL") -> pd.DataFrame:
    """
    Simulates the raw DataFrame returned by yfinance.Ticker.history().
    Index = datetime, columns = Open / High / Low / Close / Volume.
    """
    dates = pd.date_range("2024-01-02", periods=n, freq="B", tz="UTC")
    return pd.DataFrame(
        {
            "Open":   [185.0 + i for i in range(n)],
            "High":   [186.0 + i for i in range(n)],
            "Low":    [184.0 + i for i in range(n)],
            "Close":  [185.5 + i for i in range(n)],
            "Volume": [55_000_000 + i * 100_000 for i in range(n)],
        },
        index=pd.Index(dates, name="Date"),
    )


def make_canonical_ohlcv_polars(n: int = 5, symbol: str = "AAPL") -> pl.DataFrame:
    """Canonical Polars DataFrame as it would be after normalisation."""
    dates = pd.date_range("2024-01-02", periods=n, freq="B", tz="UTC")
    return pl.DataFrame(
        {
            "timestamp": [d.to_pydatetime() for d in dates],
            "open":      [185.0 + i for i in range(n)],
            "high":      [186.0 + i for i in range(n)],
            "low":       [184.0 + i for i in range(n)],
            "close":     [185.5 + i for i in range(n)],
            "volume":    [float(55_000_000 + i * 100_000) for i in range(n)],
            "symbol":    [symbol] * n,
            "timeframe": ["1d"] * n,
            "provider":  ["yahoo"] * n,
        },
        schema={
            "timestamp": pl.Datetime("us", "UTC"),
            "open":      pl.Float64,
            "high":      pl.Float64,
            "low":       pl.Float64,
            "close":     pl.Float64,
            "volume":    pl.Float64,
            "symbol":    pl.Utf8,
            "timeframe": pl.Utf8,
            "provider":  pl.Utf8,
        },
    )


# ── News ──────────────────────────────────────────────────────────────────────


def make_raw_news_pandas(n: int = 3, symbol: str = "AAPL") -> pd.DataFrame:
    """Simulates the raw DataFrame returned by NewsProvider."""
    base = datetime(2024, 1, 2, 14, 30, 0, tzinfo=timezone.utc)
    return pd.DataFrame(
        {
            "headline":     [f"News headline {i} for {symbol}" for i in range(n)],
            "published_at": [
                base.replace(hour=base.hour + i) for i in range(n)
            ],
            "source":       ["Reuters"] * n,
            "url":          [f"https://reuters.com/article/{i}" for i in range(n)],
            "symbol":       [symbol] * n,
        }
    )


# ── Fundamentals ──────────────────────────────────────────────────────────────


def make_raw_fundamentals_dict(symbol: str = "AAPL") -> dict:
    """Simulates the raw dict returned by YahooProvider.download_fundamentals()."""
    return {
        "symbol":            symbol,
        "pe_ratio":          28.4,
        "pb_ratio":          4.2,
        "revenue_growth":    0.08,
        "earnings_surprise": 0.03,
        "market_cap":        2_850_000_000_000.0,
        "eps":               6.42,
        "as_of":             "2024-01-02",
    }


# ── Macro ─────────────────────────────────────────────────────────────────────


def make_raw_macro_pandas(series: str = "CPI", n: int = 12) -> pd.DataFrame:
    """Simulates the raw DataFrame returned by FREDProvider.download_macro()."""
    dates = pd.date_range("2023-01-01", periods=n, freq="MS", tz="UTC")
    return pd.DataFrame(
        {
            "date":   dates,
            "series": [series] * n,
            "value":  [296.8 + i * 0.3 for i in range(n)],
        }
    )


# ── OHLCV delivery response format ───────────────────────────────────────────


def make_ohlcv_delivery_rows(n: int = 5) -> list[dict]:
    """The exact shape the Research Layer expects from GET /api/v1/ohlcv."""
    return [
        {
            "timestamp": f"2024-01-0{2 + i}T00:00:00",
            "open":  185.0 + i,
            "high":  186.0 + i,
            "low":   184.0 + i,
            "close": 185.5 + i,
            "volume": float(55_000_000 + i * 100_000),
        }
        for i in range(n)
    ]
