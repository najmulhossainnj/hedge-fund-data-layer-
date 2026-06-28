"""
market_data_client.py — Research Layer interface to the Data Service.

DROP THIS FILE into:
  app/engines/feature_engine/market_data_client.py

This is the ONLY file in the Research Layer that knows the Data Service exists.
All feature plugins, ML pipelines, and backtesting engines call this client.
They never call Yahoo Finance, Polygon, or any provider directly.

Changes from the original version:
  - Added X-API-Key header on every request (required by Data Service auth layer).
  - Added DATA_SERVICE_API_KEY to the environment variables read at startup.
  - Added timeout and retry handling.
  - Added structured error messages matching the Data Service error contract.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

DATA_SERVICE_BASE_URL = os.environ.get(
    "DATA_SERVICE_URL", "http://localhost:8001"
).rstrip("/")

DATA_SERVICE_API_KEY = os.environ.get(
    "DATA_SERVICE_API_KEY", "dev-api-key-change-in-production"
)

_DEFAULT_TIMEOUT = 30.0     # seconds — ingestion can take a few seconds on cache miss
_DEFAULT_RETRIES = 2

_HEADERS = {
    "X-API-Key": DATA_SERVICE_API_KEY,
    "Accept":    "application/json",
}


# ── OHLCV ─────────────────────────────────────────────────────────────────────


def get_ohlcv(
    symbol: str,
    timeframe: str = "1d",
    start: str = "2020-01-01",
    end: str = "2025-01-01",
) -> pd.DataFrame:
    """
    Fetch OHLCV bars from the Data Service.

    Returns a DataFrame indexed by timestamp with columns:
      [open, high, low, close, volume]

    Raises:
      ValueError  — if the Data Service returns no data for the range.
      RuntimeError — if the Data Service is unreachable or returns an error.
    """
    url = f"{DATA_SERVICE_BASE_URL}/api/v1/ohlcv"
    params = {"symbol": symbol, "timeframe": timeframe, "start": start, "end": end}

    rows = _get_json(url, params, context=f"OHLCV {symbol} {timeframe}")

    if not rows:
        raise ValueError(
            f"No OHLCV data returned for {symbol} {timeframe} "
            f"between {start} and {end}. "
            "Check that the symbol and date range are valid."
        )

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp").sort_index()
    return df[["open", "high", "low", "close", "volume"]]


# ── News ──────────────────────────────────────────────────────────────────────


def get_news(
    symbol: str,
    start: str = "2020-01-01",
    end: str = "2025-01-01",
) -> pd.DataFrame:
    """
    Fetch news articles from the Data Service.

    Returns a DataFrame with at least:
      [headline, published_at]

    The FinBERT pipeline uses:
      text_col    = "headline"
      date_col    = "published_at"

    Returns an empty DataFrame if no news is available — the sentiment
    pipeline handles this gracefully and returns zero scores.
    """
    url = f"{DATA_SERVICE_BASE_URL}/api/v1/news"
    params = {"symbol": symbol, "start": start, "end": end}

    rows = _get_json(url, params, context=f"news {symbol}")
    return pd.DataFrame(rows)


# ── Fundamentals ──────────────────────────────────────────────────────────────


def get_fundamentals(symbol: str) -> dict:
    """
    Fetch latest fundamental metrics for a symbol.

    Returns a dict with keys:
      symbol, pe_ratio, pb_ratio, revenue_growth, earnings_surprise,
      market_cap, eps, as_of

    Values are None when the provider does not supply them.

    Raises:
      RuntimeError — if the Data Service returns an error.
    """
    url = f"{DATA_SERVICE_BASE_URL}/api/v1/fundamentals"
    params = {"symbol": symbol}
    return _get_json(url, params, context=f"fundamentals {symbol}")


# ── Macro ─────────────────────────────────────────────────────────────────────


def get_macro(
    series: str,
    start: str = "2020-01-01",
    end: str = "2025-01-01",
) -> pd.DataFrame:
    """
    Fetch a macro time series from the Data Service.

    Args:
        series: One of CPI, FED_FUNDS_RATE, GDP_GROWTH, UNEMPLOYMENT.

    Returns a DataFrame with columns: [date, series, value]
    Returns an empty DataFrame if no data is available for the range.
    """
    url = f"{DATA_SERVICE_BASE_URL}/api/v1/macro"
    params = {"series": series, "start": start, "end": end}

    rows = _get_json(url, params, context=f"macro {series}")
    if not rows:
        return pd.DataFrame(columns=["date", "series", "value"])
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df


# ── Internal HTTP helper ──────────────────────────────────────────────────────


def _get_json(url: str, params: dict, *, context: str) -> list | dict:
    """
    Make a GET request with retry logic.

    Raises RuntimeError on persistent failure.
    """
    last_exc: Optional[Exception] = None

    for attempt in range(1, _DEFAULT_RETRIES + 2):
        try:
            with httpx.Client(timeout=_DEFAULT_TIMEOUT) as client:
                resp = client.get(url, params=params, headers=_HEADERS)

            if resp.status_code == 200:
                return resp.json()

            # Non-200 responses — extract detail and raise
            try:
                detail = resp.json().get("detail", resp.text)
            except Exception:
                detail = resp.text

            if resp.status_code == 400:
                raise ValueError(f"Bad request for {context}: {detail}")
            if resp.status_code == 401:
                raise RuntimeError(
                    f"Data Service auth failed for {context}. "
                    "Check DATA_SERVICE_API_KEY environment variable."
                )
            if resp.status_code == 404:
                raise ValueError(f"Not found: {context} — {detail}")
            if resp.status_code == 429:
                raise RuntimeError(f"Rate limited for {context}: {detail}")
            if resp.status_code == 503:
                raise RuntimeError(f"Data Service upstream unavailable for {context}: {detail}")

            raise RuntimeError(f"Unexpected status {resp.status_code} for {context}: {detail}")

        except (ValueError, RuntimeError):
            raise   # don't retry client errors — only network errors
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Data Service request failed (attempt %d/%d) for %s: %s",
                attempt, _DEFAULT_RETRIES + 1, context, exc,
            )

    raise RuntimeError(
        f"Data Service unreachable after {_DEFAULT_RETRIES + 1} attempts for {context}: {last_exc}"
    )


# ── Async variants (for async feature pipelines) ──────────────────────────────


async def get_ohlcv_async(
    symbol: str,
    timeframe: str = "1d",
    start: str = "2020-01-01",
    end: str = "2025-01-01",
) -> pd.DataFrame:
    """Async version of get_ohlcv — uses httpx.AsyncClient."""
    import asyncio
    return await asyncio.to_thread(get_ohlcv, symbol, timeframe, start, end)


async def get_news_async(symbol: str, start: str, end: str) -> pd.DataFrame:
    import asyncio
    return await asyncio.to_thread(get_news, symbol, start, end)


async def get_fundamentals_async(symbol: str) -> dict:
    import asyncio
    return await asyncio.to_thread(get_fundamentals, symbol)


async def get_macro_async(series: str, start: str, end: str) -> pd.DataFrame:
    import asyncio
    return await asyncio.to_thread(get_macro, series, start, end)
