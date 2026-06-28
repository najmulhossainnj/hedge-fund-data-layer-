"""
BaseProvider — every market data provider must implement this interface.

Rules:
- No provider-specific logic leaks outside this package.
- All methods are async (wrap sync SDKs with asyncio.to_thread).
- Retry logic lives inside each provider, not in the pipeline.
- Circuit-breaker state is stored in Redis (key: cb:{provider_name}).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class ProviderError(Exception):
    """Base class for all provider-layer errors."""


class ProviderUnavailableError(ProviderError):
    """Raised when the circuit breaker is open or the upstream is unreachable."""


class ProviderRateLimitError(ProviderError):
    """Raised when the upstream API returns a 429 / rate-limit signal."""


class BaseProvider(ABC):
    # Subclasses must set these
    name: str
    supported_timeframes: list[str]

    # ── Required methods ──────────────────────────────────────────────────

    @abstractmethod
    async def download_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """
        Fetch OHLCV bars for one symbol.

        Returns a raw DataFrame — columns differ per provider,
        the normalizer handles mapping to the canonical schema.

        Args:
            symbol:    Ticker symbol (e.g. "AAPL").
            timeframe: Research Layer timeframe string (e.g. "1d", "1h").
            start:     ISO date string "YYYY-MM-DD".
            end:       ISO date string "YYYY-MM-DD".
        """

    @abstractmethod
    async def download_news(
        self,
        symbol: str,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """
        Fetch news articles for one symbol.

        Must contain at minimum: headline, published_at columns.
        """

    @abstractmethod
    async def download_fundamentals(self, symbol: str) -> dict[str, Any]:
        """
        Fetch latest fundamental metrics for one symbol.

        Returns a raw dict — the normalizer maps it to Fundamental.
        """

    @abstractmethod
    async def download_macro(self, series: str, start: str, end: str) -> pd.DataFrame:
        """
        Fetch a macro time series.

        Args:
            series: Standardised series name (e.g. "CPI", "FED_FUNDS_RATE").
        """

    @abstractmethod
    async def symbols(self) -> list[str]:
        """Return the list of symbols this provider can serve."""

    @abstractmethod
    async def health(self) -> bool:
        """Return True if the upstream API is reachable."""
