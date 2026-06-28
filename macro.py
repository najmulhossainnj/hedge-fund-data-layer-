"""
MacroNormalizer — maps FRED DataFrames into the canonical MacroPoint schema.
"""

from __future__ import annotations

import logging

import pandas as pd
import polars as pl

from ingestion.normalizers.base import BaseNormalizer

logger = logging.getLogger(__name__)

_CANONICAL_SCHEMA = {
    "date":     pl.Datetime("us", "UTC"),
    "series":   pl.Utf8,
    "value":    pl.Float64,
    "provider": pl.Utf8,
}

_EMPTY_DF = pl.DataFrame(schema=_CANONICAL_SCHEMA)


class MacroNormalizer(BaseNormalizer):
    def normalize(
        self,
        df: pd.DataFrame,
        *,
        series: str,
        provider: str,
        **kwargs,
    ) -> pl.DataFrame:
        if df is None or df.empty:
            logger.warning("MacroNormalizer: empty DataFrame for series=%s", series)
            return _EMPTY_DF

        df = df.copy()

        # ── Normalise date column ─────────────────────────────────────────
        date_col = next(
            (c for c in df.columns if c in ("date", "Date", "index")),
            df.columns[0],
        )
        df = df.rename(columns={date_col: "date"})
        df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")

        # ── Normalise value column ────────────────────────────────────────
        if "value" not in df.columns and len(df.columns) >= 2:
            df = df.rename(columns={df.columns[1]: "value"})

        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["date", "value"])

        df["series"] = series.upper()
        df["provider"] = provider

        lf = pl.from_pandas(df[["date", "series", "value", "provider"]])
        lf = lf.with_columns(
            [
                pl.col("date").cast(pl.Datetime("us", "UTC")),
                pl.col("value").cast(pl.Float64),
            ]
        )
        lf = lf.sort("date")

        logger.info("MacroNormalizer: series=%s rows=%d", series, len(lf))
        return lf
