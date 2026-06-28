"""
FundamentalsNormalizer — maps raw provider dict into the canonical Fundamental schema.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

_CANONICAL_SCHEMA = {
    "symbol":            pl.Utf8,
    "as_of":             pl.Datetime("us", "UTC"),
    "pe_ratio":          pl.Float64,
    "pb_ratio":          pl.Float64,
    "revenue_growth":    pl.Float64,
    "earnings_surprise": pl.Float64,
    "market_cap":        pl.Float64,
    "eps":               pl.Float64,
    "provider":          pl.Utf8,
}


class FundamentalsNormalizer:
    def normalize(
        self,
        raw: dict[str, Any],
        *,
        symbol: str,
        provider: str,
    ) -> pl.DataFrame:
        """Convert a raw dict from a provider into a single-row Polars DataFrame."""
        as_of_raw = raw.get("as_of")
        if as_of_raw:
            try:
                as_of = datetime.fromisoformat(str(as_of_raw)).replace(tzinfo=timezone.utc)
            except ValueError:
                as_of = datetime.now(timezone.utc)
        else:
            as_of = datetime.now(timezone.utc)

        row = {
            "symbol":            symbol,
            "as_of":             as_of,
            "pe_ratio":          _safe_float(raw.get("pe_ratio")),
            "pb_ratio":          _safe_float(raw.get("pb_ratio")),
            "revenue_growth":    _safe_float(raw.get("revenue_growth")),
            "earnings_surprise": _safe_float(raw.get("earnings_surprise")),
            "market_cap":        _safe_float(raw.get("market_cap")),
            "eps":               _safe_float(raw.get("eps")),
            "provider":          provider,
        }

        df = pl.DataFrame([row], schema=_CANONICAL_SCHEMA)
        logger.info("FundamentalsNormalizer: %s as_of=%s", symbol, as_of.date())
        return df


def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None
