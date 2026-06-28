"""
FREDProvider — Federal Reserve Economic Data via fredapi.

Requires FRED_API_KEY environment variable (free at fred.stlouisfed.org).

Supported series:
  CPI             → CPIAUCSL
  FED_FUNDS_RATE  → FEDFUNDS
  GDP_GROWTH      → A191RL1Q225SBEA
  UNEMPLOYMENT    → UNRATE
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pandas as pd

from ingestion.providers.base import BaseProvider, ProviderError
from shared.config import settings

logger = logging.getLogger(__name__)

# Standardised series name → FRED series ID
_SERIES_MAP: dict[str, str] = {
    "CPI":            "CPIAUCSL",
    "FED_FUNDS_RATE": "FEDFUNDS",
    "GDP_GROWTH":     "A191RL1Q225SBEA",
    "UNEMPLOYMENT":   "UNRATE",
}


class FREDProvider(BaseProvider):
    name = "fred"
    supported_timeframes = []

    async def download_macro(self, series: str, start: str, end: str) -> pd.DataFrame:
        if not settings.FRED_API_KEY:
            raise ProviderError(
                "FRED_API_KEY is not set — cannot fetch macro data from FRED."
            )

        fred_series_id = _SERIES_MAP.get(series.upper())
        if fred_series_id is None:
            raise ProviderError(
                f"Unknown macro series '{series}'. "
                f"Supported: {list(_SERIES_MAP.keys())}"
            )

        def _sync() -> pd.DataFrame:
            from fredapi import Fred
            fred = Fred(api_key=settings.FRED_API_KEY)
            s = fred.get_series(fred_series_id, observation_start=start, observation_end=end)
            if s is None or s.empty:
                return pd.DataFrame(columns=["date", "series", "value"])
            df = s.reset_index()
            df.columns = ["date", "value"]
            df["series"] = series.upper()
            df["date"] = pd.to_datetime(df["date"])
            df = df.dropna(subset=["value"])
            return df[["date", "series", "value"]]

        try:
            df = await asyncio.to_thread(_sync)
            logger.info("FREDProvider: series=%s rows=%d", series, len(df))
            return df
        except ProviderError:
            raise
        except Exception as exc:
            raise ProviderError(f"FRED fetch failed for series={series}: {exc}") from exc

    async def download_ohlcv(self, symbol: str, timeframe: str, start: str, end: str) -> pd.DataFrame:
        raise NotImplementedError("FREDProvider does not implement OHLCV.")

    async def download_news(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        raise NotImplementedError("FREDProvider does not implement news.")

    async def download_fundamentals(self, symbol: str) -> dict[str, Any]:
        raise NotImplementedError("FREDProvider does not implement fundamentals.")

    async def symbols(self) -> list[str]:
        return list(_SERIES_MAP.keys())

    async def health(self) -> bool:
        return bool(settings.FRED_API_KEY)
