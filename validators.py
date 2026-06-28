"""
Data quality validators.

All validators:
  - Accept a Polars DataFrame
  - Return (passed: bool, issues: list[str])
  - NEVER silently drop data except empty headlines (handled in normalizer)
  - Log every issue as a structured warning

Run all validators via run_all_ohlcv() / run_all_news().
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import polars as pl

logger = logging.getLogger(__name__)

ValidationResult = tuple[bool, list[str]]


# ── OHLCV validators ──────────────────────────────────────────────────────────


def check_price_range(df: pl.DataFrame) -> ValidationResult:
    """high must be >= low; close must be in [low, high]."""
    issues: list[str] = []

    hl_bad = df.filter(pl.col("high") < pl.col("low"))
    if len(hl_bad) > 0:
        issues.append(
            f"check_price_range: {len(hl_bad)} rows where high < low"
        )

    close_bad = df.filter(
        (pl.col("close") < pl.col("low")) | (pl.col("close") > pl.col("high"))
    )
    if len(close_bad) > 0:
        issues.append(
            f"check_price_range: {len(close_bad)} rows where close is outside [low, high]"
        )

    passed = len(issues) == 0
    if not passed:
        for issue in issues:
            logger.warning("Data quality: %s", issue)
    return passed, issues


def check_zero_volume(df: pl.DataFrame) -> ValidationResult:
    """Warn (do not drop) on zero-volume bars."""
    zero_vol = df.filter(pl.col("volume") == 0)
    issues: list[str] = []
    if len(zero_vol) > 0:
        msg = f"check_zero_volume: {len(zero_vol)} bars with volume=0 (warning only)"
        issues.append(msg)
        logger.warning("Data quality: %s", msg)
    return True, issues  # always passes — zero volume is a warning, not an error


def check_price_spike(df: pl.DataFrame, threshold: float = 0.20) -> ValidationResult:
    """Flag bars where any price field changes by more than threshold from prior close."""
    issues: list[str] = []
    if len(df) < 2:
        return True, issues

    prior_close = df["close"].shift(1)
    for col in ("open", "high", "low", "close"):
        pct_change = ((df[col] - prior_close) / prior_close).abs()
        spike_count = (pct_change > threshold).sum()
        if spike_count and spike_count > 0:
            msg = (
                f"check_price_spike: {spike_count} {col} values "
                f"differ from prior close by >{threshold*100:.0f}%"
            )
            issues.append(msg)
            logger.warning("Data quality: %s", msg)

    return len(issues) == 0, issues


def check_gaps(df: pl.DataFrame, symbol: str) -> ValidationResult:
    """Detect missing trading days for daily data using NYSE calendar."""
    issues: list[str] = []
    if len(df) < 2:
        return True, issues

    try:
        import pandas_market_calendars as mcal
        import pandas as pd

        ts = df["timestamp"].to_pandas()
        start = ts.min().date()
        end = ts.max().date()

        nyse = mcal.get_calendar("NYSE")
        schedule = nyse.schedule(
            start_date=start.isoformat(), end_date=end.isoformat()
        )
        expected_dates = set(schedule.index.date)
        actual_dates = set(ts.dt.date)

        missing = expected_dates - actual_dates
        if missing:
            sample = sorted(missing)[:5]
            msg = (
                f"check_gaps: {symbol} missing {len(missing)} NYSE trading days "
                f"(sample: {[str(d) for d in sample]})"
            )
            issues.append(msg)
            logger.warning("Data quality: %s", msg)
    except Exception as exc:
        logger.warning("check_gaps: could not run calendar check — %s", exc)

    return len(issues) == 0, issues


def check_duplicates(df: pl.DataFrame) -> ValidationResult:
    """Assert no duplicate timestamps per dataset."""
    issues: list[str] = []
    n_total = len(df)
    n_unique = df["timestamp"].n_unique()
    if n_unique < n_total:
        msg = f"check_duplicates: {n_total - n_unique} duplicate timestamps found"
        issues.append(msg)
        logger.warning("Data quality: %s", msg)
    return len(issues) == 0, issues


def check_negative_prices(df: pl.DataFrame) -> ValidationResult:
    """No price field should be negative."""
    issues: list[str] = []
    for col in ("open", "high", "low", "close"):
        neg = df.filter(pl.col(col) < 0)
        if len(neg) > 0:
            msg = f"check_negative_prices: {len(neg)} rows with {col} < 0"
            issues.append(msg)
            logger.warning("Data quality: %s", msg)
    return len(issues) == 0, issues


# ── News validators ───────────────────────────────────────────────────────────


def check_future_timestamps(df: pl.DataFrame) -> ValidationResult:
    """Flag articles with published_at in the future."""
    issues: list[str] = []
    now_utc = datetime.now(timezone.utc)

    future = df.filter(
        pl.col("published_at").cast(pl.Datetime("us", "UTC")) > pl.lit(now_utc).cast(pl.Datetime("us", "UTC"))
    )
    if len(future) > 0:
        msg = f"check_future_timestamps: {len(future)} articles with future published_at"
        issues.append(msg)
        logger.warning("Data quality: %s", msg)
    return len(issues) == 0, issues


def check_empty_headlines(df: pl.DataFrame) -> ValidationResult:
    """Verify no empty headlines leaked through the normalizer."""
    issues: list[str] = []
    empty = df.filter(pl.col("headline").str.strip_chars().str.len_chars() == 0)
    if len(empty) > 0:
        msg = f"check_empty_headlines: {len(empty)} empty headlines (should have been filtered)"
        issues.append(msg)
        logger.warning("Data quality: %s", msg)
    return len(issues) == 0, issues


# ── Aggregate runners ─────────────────────────────────────────────────────────


def run_all_ohlcv(df: pl.DataFrame, symbol: str) -> tuple[bool, list[str]]:
    """Run all OHLCV validators. Returns (all_passed, all_issues)."""
    all_issues: list[str] = []
    all_passed = True

    for validator_fn, kwargs in [
        (check_duplicates, {}),
        (check_negative_prices, {}),
        (check_price_range, {}),
        (check_zero_volume, {}),
        (check_price_spike, {}),
        (check_gaps, {"symbol": symbol}),
    ]:
        try:
            passed, issues = validator_fn(df, **kwargs)
            all_issues.extend(issues)
            if not passed:
                all_passed = False
        except Exception as exc:
            msg = f"{validator_fn.__name__}: unexpected error — {exc}"
            all_issues.append(msg)
            logger.error("Data quality validator error: %s", msg)

    return all_passed, all_issues


def run_all_news(df: pl.DataFrame) -> tuple[bool, list[str]]:
    """Run all news validators. Returns (all_passed, all_issues)."""
    all_issues: list[str] = []
    all_passed = True

    for validator_fn in [check_future_timestamps, check_empty_headlines]:
        try:
            passed, issues = validator_fn(df)
            all_issues.extend(issues)
            if not passed:
                all_passed = False
        except Exception as exc:
            msg = f"{validator_fn.__name__}: unexpected error — {exc}"
            all_issues.append(msg)
            logger.error("Data quality validator error: %s", msg)

    return all_passed, all_issues
