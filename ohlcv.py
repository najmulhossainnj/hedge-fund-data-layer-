"""
OHLCVNormalizer — maps yfinance (and future provider) DataFrames into the
canonical Candle schema as a Polars DataFrame.

Handles:
- Column renaming (Open/High/Low/Close/Volume → lowercase)
- Index reset (yfinance sets Date/Datetime as index)
- Timezone normalisation → UTC
- Type casting
- Deduplication
- Sort by timestamp ascending
"""

from __future__ import annotations

import logging

import pandas as pd
import polars as pl

from ingestion.normalizers.base import BaseNormalizer

logger = logging.getLogger(__name__)

# Known column aliases per provider (add more as new providers are integrated)
_COLUMN_ALIASES: dict[str, str] = {
    # yfinance column names
    "Open":   "open",
    "High":   "high",
    "Low":    "low",
    "Close":  "close",
    "Volume": "volume",
    # Generic fallbacks
    "open":   "open",
    "high":   "high",
    "low":    "low",
    "close":  "close",
    "volume": "volume",
}

_CANONICAL_SCHEMA = {
    "timestamp": pl.Datetime("us", "UTC"),
    "open":      pl.Float64,
    "high":      pl.Float64,
    "low":       pl.Float64,
    "close":     pl.Float64,
    "volume":    pl.Float64,
    "symbol":    pl.Utf8,
    "timeframe": pl.Utf8,
    "provider":  pl.Utf8,
}

_EMPTY_DF = pl.DataFrame(schema=_CANONICAL_SCHEMA)


class OHLCVNormalizer(BaseNormalizer):
    def normalize(
        self,
        df: pd.DataFrame,
        *,
        symbol: str,
        timeframe: str,
        provider: str,
        **kwargs,
    ) -> pl.DataFrame:
        if df is None or df.empty:
            logger.warning("OHLCVNormalizer: empty DataFrame for %s %s", symbol, timeframe)
            return _EMPTY_DF

        # ── 1. Reset index so Date/Datetime becomes a regular column ──────
        df = df.reset_index()

        # ── 2. Identify the timestamp column ─────────────────────────────
        ts_col = None
        for candidate in ("Datetime", "Date", "datetime", "date", "timestamp"):
            if candidate in df.columns:
                ts_col = candidate
                break
        if ts_col is None:
            logger.error("OHLCVNormalizer: no timestamp column found in %s", list(df.columns))
            return _EMPTY_DF

        df = df.rename(columns={ts_col: "timestamp"})

        # ── 3. Rename OHLCV columns ───────────────────────────────────────
        rename_map = {
            col: _COLUMN_ALIASES[col]
            for col in df.columns
            if col in _COLUMN_ALIASES and col != _COLUMN_ALIASES[col]
        }
        df = df.rename(columns=rename_map)

        # ── 4. Keep only canonical columns ───────────────────────────────
        required = ["timestamp", "open", "high", "low", "close", "volume"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            logger.error("OHLCVNormalizer: missing columns %s for %s", missing, symbol)
            return _EMPTY_DF
        df = df[required].copy()

        # ── 5. Normalise timestamp to UTC ─────────────────────────────────
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        # ── 6. Cast numeric types ─────────────────────────────────────────
        for col in ("open", "high", "low", "close", "volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # ── 7. Drop rows with nulls in price columns ─────────────────────
        df = df.dropna(subset=["open", "high", "low", "close"])

        # ── 8. Convert to Polars ──────────────────────────────────────────
        lf = pl.from_pandas(df)

        # ── 9. Add metadata columns ───────────────────────────────────────
        lf = lf.with_columns(
            [
                pl.lit(symbol).alias("symbol"),
                pl.lit(timeframe).alias("timeframe"),
                pl.lit(provider).alias("provider"),
            ]
        )

        # ── 10. Ensure correct Polars types ───────────────────────────────
        lf = lf.with_columns(
            [
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume").cast(pl.Float64),
                pl.col("timestamp").cast(pl.Datetime("us", "UTC")),
            ]
        )

        # ── 11. Deduplicate on timestamp ──────────────────────────────────
        lf = lf.unique(subset=["timestamp"], keep="first")

        # ── 12. Sort ascending ────────────────────────────────────────────
        lf = lf.sort("timestamp")

        logger.info(
            "OHLCVNormalizer: %s %s → %d rows", symbol, timeframe, len(lf)
        )
        return lf
