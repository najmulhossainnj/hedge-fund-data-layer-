"""
Unit tests for all data quality validators.

Each validator is tested in isolation with crafted Polars DataFrames.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from ingestion.quality.validators import (
    check_duplicates,
    check_empty_headlines,
    check_future_timestamps,
    check_negative_prices,
    check_price_range,
    check_price_spike,
    check_zero_volume,
    run_all_news,
    run_all_ohlcv,
)
from tests.fixtures.sample_data import make_canonical_ohlcv_polars


def _make_ohlcv(**overrides) -> pl.DataFrame:
    """Create a minimal OHLCV DataFrame with optional field overrides."""
    base = make_canonical_ohlcv_polars(n=5)
    for col, values in overrides.items():
        base = base.with_columns(pl.Series(col, values))
    return base


def _make_news(n: int = 3, future: bool = False) -> pl.DataFrame:
    now = datetime.now(timezone.utc)
    delta = timedelta(hours=1) if not future else -timedelta(hours=-1)
    published = [now + timedelta(hours=i) * (1 if future else -1) for i in range(n)]
    return pl.DataFrame(
        {
            "headline":     [f"Headline {i}" for i in range(n)],
            "published_at": published,
        },
        schema={"headline": pl.Utf8, "published_at": pl.Datetime("us", "UTC")},
    )


# ── check_price_range ─────────────────────────────────────────────────────────


class TestCheckPriceRange:
    def test_passes_on_clean_data(self):
        df = _make_ohlcv()
        passed, issues = check_price_range(df)
        assert passed
        assert issues == []

    def test_fails_when_high_lt_low(self):
        df = make_canonical_ohlcv_polars(n=5)
        # Swap high and low on row 2
        highs = df["high"].to_list()
        lows = df["low"].to_list()
        highs[2], lows[2] = lows[2], highs[2]   # high < low now
        df = df.with_columns([pl.Series("high", highs), pl.Series("low", lows)])
        passed, issues = check_price_range(df)
        assert not passed
        assert any("high < low" in i for i in issues)

    def test_fails_when_close_outside_range(self):
        df = make_canonical_ohlcv_polars(n=3)
        closes = df["close"].to_list()
        closes[0] = df["high"][0] + 10.0   # close > high
        df = df.with_columns(pl.Series("close", closes))
        passed, issues = check_price_range(df)
        assert not passed


# ── check_zero_volume ─────────────────────────────────────────────────────────


class TestCheckZeroVolume:
    def test_always_passes_even_with_zero_volume(self):
        """Zero volume is a warning, never an error."""
        df = make_canonical_ohlcv_polars(n=3).with_columns(
            pl.Series("volume", [0.0, 0.0, 0.0])
        )
        passed, issues = check_zero_volume(df)
        assert passed          # always True
        assert len(issues) == 1
        assert "zero" in issues[0].lower()

    def test_no_issue_when_volume_nonzero(self):
        df = make_canonical_ohlcv_polars(n=3)
        passed, issues = check_zero_volume(df)
        assert passed
        assert issues == []


# ── check_price_spike ─────────────────────────────────────────────────────────


class TestCheckPriceSpike:
    def test_passes_on_normal_data(self):
        df = make_canonical_ohlcv_polars(n=10)
        passed, issues = check_price_spike(df)
        assert passed

    def test_detects_spike(self):
        df = make_canonical_ohlcv_polars(n=5)
        closes = df["close"].to_list()
        closes[3] = closes[2] * 2.0    # 100% spike
        df = df.with_columns(pl.Series("close", closes))
        passed, issues = check_price_spike(df, threshold=0.20)
        assert not passed

    def test_single_row_always_passes(self):
        df = make_canonical_ohlcv_polars(n=1)
        passed, issues = check_price_spike(df)
        assert passed


# ── check_duplicates ──────────────────────────────────────────────────────────


class TestCheckDuplicates:
    def test_passes_on_unique_timestamps(self):
        df = make_canonical_ohlcv_polars(n=5)
        passed, issues = check_duplicates(df)
        assert passed

    def test_fails_on_duplicate_timestamps(self):
        df = make_canonical_ohlcv_polars(n=3)
        df_duped = pl.concat([df, df])   # duplicate everything
        passed, issues = check_duplicates(df_duped)
        assert not passed
        assert any("duplicate" in i.lower() for i in issues)


# ── check_negative_prices ─────────────────────────────────────────────────────


class TestCheckNegativePrices:
    def test_passes_on_positive_prices(self):
        df = make_canonical_ohlcv_polars(n=5)
        passed, issues = check_negative_prices(df)
        assert passed

    def test_fails_on_negative_open(self):
        df = make_canonical_ohlcv_polars(n=3)
        opens = df["open"].to_list()
        opens[1] = -5.0
        df = df.with_columns(pl.Series("open", opens))
        passed, issues = check_negative_prices(df)
        assert not passed


# ── News validators ───────────────────────────────────────────────────────────


class TestNewsValidators:
    def test_future_timestamps_detected(self):
        future = datetime.now(timezone.utc) + timedelta(days=1)
        df = pl.DataFrame(
            {"headline": ["Future news"], "published_at": [future]},
            schema={"headline": pl.Utf8, "published_at": pl.Datetime("us", "UTC")},
        )
        passed, issues = check_future_timestamps(df)
        assert not passed

    def test_past_timestamps_pass(self):
        past = datetime.now(timezone.utc) - timedelta(days=1)
        df = pl.DataFrame(
            {"headline": ["Old news"], "published_at": [past]},
            schema={"headline": pl.Utf8, "published_at": pl.Datetime("us", "UTC")},
        )
        passed, issues = check_future_timestamps(df)
        assert passed

    def test_empty_headline_check(self):
        df = pl.DataFrame(
            {"headline": ["", "  ", "Real headline"]},
            schema={"headline": pl.Utf8},
        )
        # The validator should detect empty / whitespace-only headlines
        # that should have been filtered by the normalizer
        passed, issues = check_empty_headlines(df)
        assert not passed


# ── run_all_ohlcv ─────────────────────────────────────────────────────────────


class TestRunAllOHLCV:
    def test_clean_data_all_pass(self):
        df = make_canonical_ohlcv_polars(n=5)
        passed, issues = run_all_ohlcv(df, symbol="AAPL")
        # May have zero-volume warnings but no hard failures
        for issue in issues:
            assert "error" not in issue.lower()

    def test_bad_data_collects_all_issues(self):
        df = make_canonical_ohlcv_polars(n=5)
        # Inject two different problems
        closes = df["close"].to_list()
        closes[0] = df["high"][0] + 100.0   # close out of range
        highs = df["high"].to_list()
        lows = df["low"].to_list()
        highs[1], lows[1] = lows[1], highs[1]  # high < low
        df = df.with_columns([
            pl.Series("close", closes),
            pl.Series("high", highs),
            pl.Series("low", lows),
        ])
        passed, issues = run_all_ohlcv(df, symbol="AAPL")
        assert not passed
        assert len(issues) >= 1


# ── run_all_news ──────────────────────────────────────────────────────────────


class TestRunAllNews:
    def test_clean_news_passes(self):
        past = datetime.now(timezone.utc) - timedelta(hours=2)
        df = pl.DataFrame(
            {"headline": ["Good news"], "published_at": [past]},
            schema={"headline": pl.Utf8, "published_at": pl.Datetime("us", "UTC")},
        )
        passed, issues = run_all_news(df)
        assert passed

    def test_future_news_fails(self):
        future = datetime.now(timezone.utc) + timedelta(days=1)
        df = pl.DataFrame(
            {"headline": ["Future news"], "published_at": [future]},
            schema={"headline": pl.Utf8, "published_at": pl.Datetime("us", "UTC")},
        )
        passed, issues = run_all_news(df)
        assert not passed
