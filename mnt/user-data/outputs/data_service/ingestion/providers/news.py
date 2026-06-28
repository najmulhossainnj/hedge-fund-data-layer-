"""
NewsProvider — fetches news headlines for a symbol.

Primary:  NewsAPI (newsapi.org) — requires NEWSAPI_KEY env var.
Fallback: Yahoo Finance RSS feed via feedparser — no API key needed.

published_at is always normalised to timezone-aware UTC before returning.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import feedparser
import pandas as pd

from ingestion.providers.base import BaseProvider, ProviderError
from shared.config import settings

logger = logging.getLogger(__name__)

_YAHOO_RSS_URL = (
    "https://feeds.finance.yahoo.com/rss/2.0/headline"
    "?s={symbol}&region=US&lang=en-US"
)


def _to_utc(dt: datetime) -> datetime:
    """Ensure a datetime is UTC and timezone-aware."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class NewsProvider(BaseProvider):
    name = "news"
    supported_timeframes = []       # news is not timeframe-based

    # ── NewsAPI (primary) ─────────────────────────────────────────────────

    async def _fetch_newsapi(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        if not settings.NEWSAPI_KEY:
            raise ProviderError("NEWSAPI_KEY is not set — cannot use NewsAPI.")

        def _sync() -> list[dict[str, Any]]:
            from newsapi import NewsApiClient
            client = NewsApiClient(api_key=settings.NEWSAPI_KEY)
            response = client.get_everything(
                q=symbol,
                from_param=start,
                to=end,
                language="en",
                sort_by="publishedAt",
                page_size=100,
            )
            articles = response.get("articles", [])
            rows: list[dict[str, Any]] = []
            for art in articles:
                published_raw = art.get("publishedAt")
                if not published_raw:
                    continue
                try:
                    published_at = _to_utc(datetime.fromisoformat(published_raw.rstrip("Z")))
                except ValueError:
                    continue
                rows.append(
                    {
                        "headline": art.get("title") or "",
                        "published_at": published_at,
                        "source": (art.get("source") or {}).get("name"),
                        "url": art.get("url"),
                        "symbol": symbol,
                    }
                )
            return rows

        rows = await asyncio.to_thread(_sync)
        return pd.DataFrame(rows)

    # ── Yahoo Finance RSS (fallback) ──────────────────────────────────────

    async def _fetch_rss(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        url = _YAHOO_RSS_URL.format(symbol=symbol)

        def _sync() -> list[dict[str, Any]]:
            feed = feedparser.parse(url)
            start_dt = _to_utc(datetime.fromisoformat(start))
            end_dt = _to_utc(datetime.fromisoformat(end).replace(
                hour=23, minute=59, second=59
            ))
            rows: list[dict[str, Any]] = []
            for entry in feed.entries:
                try:
                    published_at = _to_utc(parsedate_to_datetime(entry.get("published", "")))
                except Exception:
                    continue
                if not (start_dt <= published_at <= end_dt):
                    continue
                rows.append(
                    {
                        "headline": entry.get("title", ""),
                        "published_at": published_at,
                        "source": "Yahoo Finance",
                        "url": entry.get("link"),
                        "symbol": symbol,
                    }
                )
            return rows

        rows = await asyncio.to_thread(_sync)
        return pd.DataFrame(rows)

    # ── Public interface ──────────────────────────────────────────────────

    async def download_news(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        """Try NewsAPI first; fall back to RSS if unavailable."""
        if settings.NEWSAPI_KEY:
            try:
                df = await self._fetch_newsapi(symbol, start, end)
                if not df.empty:
                    logger.info(
                        "NewsProvider(NewsAPI): %s articles=%d", symbol, len(df)
                    )
                    return df
            except Exception as exc:
                logger.warning(
                    "NewsAPI failed for %s, falling back to RSS: %s", symbol, exc
                )

        logger.info("NewsProvider: using RSS fallback for %s", symbol)
        df = await self._fetch_rss(symbol, start, end)
        return df

    async def download_ohlcv(self, symbol: str, timeframe: str, start: str, end: str) -> pd.DataFrame:
        raise NotImplementedError("NewsProvider does not implement OHLCV.")

    async def download_fundamentals(self, symbol: str) -> dict[str, Any]:
        raise NotImplementedError("NewsProvider does not implement fundamentals.")

    async def download_macro(self, series: str, start: str, end: str) -> pd.DataFrame:
        raise NotImplementedError("NewsProvider does not implement macro.")

    async def symbols(self) -> list[str]:
        return []

    async def health(self) -> bool:
        try:
            df = await self._fetch_rss("AAPL", "2024-01-01", "2024-01-31")
            return True     # RSS responded even if empty
        except Exception:
            return False
