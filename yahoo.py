"""
YahooProvider — wraps yfinance for OHLCV and fundamentals.

Design decisions:
- Every yfinance call is wrapped in asyncio.to_thread (blocking SDK).
- Retry: exponential back-off via tenacity, max 3 attempts.
- Circuit breaker: failure count stored in Redis. If >= threshold, raises
  ProviderUnavailableError immediately without calling the upstream.
- Never call yf.download() with multiple symbols — one symbol per call.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import redis.asyncio as aioredis
import yfinance as yf
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ingestion.providers.base import (
    BaseProvider,
    ProviderError,
    ProviderRateLimitError,
    ProviderUnavailableError,
)
from shared.config import settings

logger = logging.getLogger(__name__)

# Timeframe mapping: Research Layer string → yfinance interval
_TIMEFRAME_MAP: dict[str, str] = {
    "1m":  "1mo",   # monthly
    "1w":  "1wk",
    "1d":  "1d",
    "1h":  "1h",
    "30m": "30m",
    "15m": "15m",
    "5m":  "5m",
}

# Fundamental field mapping: yfinance key → canonical field name
_FUNDAMENTAL_MAP: dict[str, str] = {
    "trailingPE":        "pe_ratio",
    "priceToBook":       "pb_ratio",
    "revenueGrowth":     "revenue_growth",
    "earningsSurprise":  "earnings_surprise",   # note: not always present
    "marketCap":         "market_cap",
    "trailingEps":       "eps",
}

_CB_KEY_PREFIX = "cb:yahoo:"


class YahooProvider(BaseProvider):
    name = "yahoo"
    supported_timeframes = list(_TIMEFRAME_MAP.keys())

    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None

    # ── Circuit breaker ────────────────────────────────────────────────────

    async def _redis_client(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        return self._redis

    async def _check_circuit(self, key: str) -> None:
        client = await self._redis_client()
        raw = await client.get(key)
        count = int(raw) if raw else 0
        if count >= settings.CIRCUIT_BREAKER_THRESHOLD:
            raise ProviderUnavailableError(
                f"Yahoo Finance circuit breaker open for key={key}. "
                f"Failures={count} >= threshold={settings.CIRCUIT_BREAKER_THRESHOLD}"
            )

    async def _record_failure(self, key: str) -> None:
        client = await self._redis_client()
        await client.incr(key)
        await client.expire(key, settings.CIRCUIT_BREAKER_RESET_TIMEOUT)

    async def _reset_circuit(self, key: str) -> None:
        client = await self._redis_client()
        await client.delete(key)

    # ── OHLCV ─────────────────────────────────────────────────────────────

    async def download_ohlcv(
        self, symbol: str, timeframe: str, start: str, end: str
    ) -> pd.DataFrame:
        cb_key = f"{_CB_KEY_PREFIX}ohlcv:{symbol}"
        await self._check_circuit(cb_key)

        yf_interval = _TIMEFRAME_MAP.get(timeframe)
        if yf_interval is None:
            raise ProviderError(f"Unsupported timeframe '{timeframe}' for YahooProvider")

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=2, min=2, max=10),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        )
        def _fetch() -> pd.DataFrame:
            ticker = yf.Ticker(symbol)
            df = ticker.history(
                start=start,
                end=end,
                interval=yf_interval,
                auto_adjust=True,
                actions=False,
            )
            return df

        try:
            df = await asyncio.to_thread(_fetch)
            await self._reset_circuit(cb_key)
            logger.info("YahooProvider.download_ohlcv: %s %s rows=%d", symbol, timeframe, len(df))
            return df
        except Exception as exc:
            await self._record_failure(cb_key)
            logger.error("YahooProvider.download_ohlcv failed: %s — %s", symbol, exc)
            raise ProviderError(f"Yahoo Finance OHLCV fetch failed for {symbol}: {exc}") from exc

    # ── News ──────────────────────────────────────────────────────────────
    # Yahoo does not have a reliable news API via yfinance — the NewsProvider
    # handles this. We implement a minimal stub so the interface is complete.

    async def download_news(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        raise NotImplementedError(
            "YahooProvider does not implement news. Use NewsProvider."
        )

    # ── Fundamentals ──────────────────────────────────────────────────────

    async def download_fundamentals(self, symbol: str) -> dict[str, Any]:
        cb_key = f"{_CB_KEY_PREFIX}fundamentals:{symbol}"
        await self._check_circuit(cb_key)

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=2, min=2, max=10),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        )
        def _fetch() -> dict[str, Any]:
            ticker = yf.Ticker(symbol)
            info = ticker.info or {}
            result: dict[str, Any] = {"symbol": symbol}
            for yf_key, canonical_key in _FUNDAMENTAL_MAP.items():
                val = info.get(yf_key)
                if val is not None:
                    result[canonical_key] = float(val)
            result["as_of"] = datetime.now(timezone.utc).date().isoformat()
            return result

        try:
            data = await asyncio.to_thread(_fetch)
            await self._reset_circuit(cb_key)
            return data
        except Exception as exc:
            await self._record_failure(cb_key)
            raise ProviderError(
                f"Yahoo Finance fundamentals fetch failed for {symbol}: {exc}"
            ) from exc

    # ── Macro ─────────────────────────────────────────────────────────────

    async def download_macro(self, series: str, start: str, end: str) -> pd.DataFrame:
        raise NotImplementedError(
            "YahooProvider does not implement macro. Use FREDProvider."
        )

    # ── Metadata ──────────────────────────────────────────────────────────

    async def symbols(self) -> list[str]:
        # Yahoo supports essentially all major exchange symbols;
        # return a curated default list for health-check purposes only.
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "SPY", "QQQ"]

    async def health(self) -> bool:
        try:
            df = await asyncio.to_thread(
                lambda: yf.Ticker("AAPL").history(period="1d", interval="1d")
            )
            return not df.empty
        except Exception:
            return False
