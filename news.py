"""
NewsNormalizer — maps raw news DataFrames into the canonical NewsItem schema.

Guarantees:
- published_at is timezone-aware UTC datetime
- headline is a non-empty string
- Empty or invalid rows are dropped
"""

from __future__ import annotations

import logging

import pandas as pd
import polars as pl

from ingestion.normalizers.base import BaseNormalizer

logger = logging.getLogger(__name__)

_CANONICAL_SCHEMA = {
    "headline":     pl.Utf8,
    "published_at": pl.Datetime("us", "UTC"),
    "source":       pl.Utf8,
    "url":          pl.Utf8,
    "symbol":       pl.Utf8,
    "provider":     pl.Utf8,
}

_EMPTY_DF = pl.DataFrame(schema=_CANONICAL_SCHEMA)


class NewsNormalizer(BaseNormalizer):
    def normalize(
        self,
        df: pd.DataFrame,
        *,
        symbol: str,
        provider: str,
        **kwargs,
    ) -> pl.DataFrame:
        if df is None or df.empty:
            logger.warning("NewsNormalizer: empty DataFrame for %s", symbol)
            return _EMPTY_DF

        # ── 1. Ensure required columns ────────────────────────────────────
        if "headline" not in df.columns or "published_at" not in df.columns:
            logger.error(
                "NewsNormalizer: missing required columns. Got: %s", list(df.columns)
            )
            return _EMPTY_DF

        df = df.copy()

        # ── 2. Normalise published_at → UTC aware datetime ─────────────
        df["published_at"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce")

        # ── 3. Drop rows with invalid / empty headlines ───────────────────
        df = df.dropna(subset=["headline", "published_at"])
        df = df[df["headline"].str.strip().ne("")]

        if df.empty:
            return _EMPTY_DF

        # ── 4. Fill optional columns ──────────────────────────────────────
        for col in ("source", "url"):
            if col not in df.columns:
                df[col] = None

        # ── 5. Add metadata columns ───────────────────────────────────────
        df["symbol"] = symbol
        df["provider"] = provider

        # ── 6. Select canonical columns ───────────────────────────────────
        df = df[["headline", "published_at", "source", "url", "symbol", "provider"]]

        # ── 7. Convert to Polars ──────────────────────────────────────────
        lf = pl.from_pandas(df)

        lf = lf.with_columns(
            pl.col("published_at").cast(pl.Datetime("us", "UTC"))
        )

        # ── 8. Sort ascending by published_at ─────────────────────────────
        lf = lf.sort("published_at")

        logger.info("NewsNormalizer: %s → %d articles", symbol, len(lf))
        return lf
