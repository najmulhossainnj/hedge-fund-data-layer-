"""
Unit tests for all normalizers.

Tests are isolated — no providers, no DB, no MinIO.
Input = raw fixture DataFrames / dicts.
Assert = correct Polars schema and values.
"""

from __future__ import annotations

import pandas as pd
import polars as pl
import pytest

from ingestion.normalizers.fundamentals import FundamentalsNormalizer
from ingestion.normalizers.macro import MacroNormalizer
from ingestion.normalizers.news import NewsNormalizer
from ingestion.normalizers.ohlcv import OHLCVNormalizer
from tests.fixtures.sample_data import (
    make_raw_fundamentals_dict,
    make_raw_macro_pandas,
    make_raw_news_pandas,
    make_raw_ohlcv_pandas,
)


# ── OHLCV ─────────────────────────────────────────────────────────────────────


class TestOHLCVNormalizer:
    def setup_method(self):
        self.norm = OHLCVNormalizer()

    def test_basic_normalisation(self):
        raw = make_raw_ohlcv_pandas(n=5)
        result = self.norm.normalize(raw, symbol="AAPL", timeframe="1d", provider="yahoo")

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 5

    def test_column_names(self):
        raw = make_raw_ohlcv_pandas(n=3)
        result = self.norm.normalize(raw, symbol="AAPL", timeframe="1d", provider="yahoo")

        expected_cols = {"timestamp", "open", "high", "low", "close", "volume", "symbol", "timeframe", "provider"}
        assert set(result.columns) >= expected_cols

    def test_metadata_columns_added(self):
        raw = make_raw_ohlcv_pandas(n=3)
        result = self.norm.normalize(raw, symbol="MSFT", timeframe="1h", provider="yahoo")

        assert result["symbol"].to_list() == ["MSFT"] * 3
        assert result["timeframe"].to_list() == ["1h"] * 3
        assert result["provider"].to_list() == ["yahoo"] * 3

    def test_sorted_ascending(self):
        raw = make_raw_ohlcv_pandas(n=5)
        result = self.norm.normalize(raw, symbol="AAPL", timeframe="1d", provider="yahoo")

        timestamps = result["timestamp"].to_list()
        assert timestamps == sorted(timestamps)

    def test_types_are_float(self):
        raw = make_raw_ohlcv_pandas(n=3)
        result = self.norm.normalize(raw, symbol="AAPL", timeframe="1d", provider="yahoo")

        for col in ("open", "high", "low", "close", "volume"):
            assert result[col].dtype == pl.Float64, f"{col} should be Float64"

    def test_empty_dataframe_returns_empty_with_schema(self):
        empty = pd.DataFrame()
        result = self.norm.normalize(empty, symbol="AAPL", timeframe="1d", provider="yahoo")

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0
        assert "timestamp" in result.columns

    def test_deduplication(self):
        raw = make_raw_ohlcv_pandas(n=3)
        # Duplicate all rows
        raw_duped = pd.concat([raw, raw])
        result = self.norm.normalize(raw_duped, symbol="AAPL", timeframe="1d", provider="yahoo")
        assert len(result) == 3

    def test_timestamp_is_utc(self):
        raw = make_raw_ohlcv_pandas(n=2)
        result = self.norm.normalize(raw, symbol="AAPL", timeframe="1d", provider="yahoo")
        assert result["timestamp"].dtype == pl.Datetime("us", "UTC")

    def test_values_are_correct(self):
        raw = make_raw_ohlcv_pandas(n=1)
        result = self.norm.normalize(raw, symbol="AAPL", timeframe="1d", provider="yahoo")
        assert result["open"][0] == pytest.approx(185.0)
        assert result["close"][0] == pytest.approx(185.5)


# ── News ──────────────────────────────────────────────────────────────────────


class TestNewsNormalizer:
    def setup_method(self):
        self.norm = NewsNormalizer()

    def test_basic_normalisation(self):
        raw = make_raw_news_pandas(n=3)
        result = self.norm.normalize(raw, symbol="AAPL", provider="news")

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 3

    def test_required_columns_present(self):
        raw = make_raw_news_pandas(n=2)
        result = self.norm.normalize(raw, symbol="AAPL", provider="news")

        assert "headline" in result.columns
        assert "published_at" in result.columns

    def test_published_at_is_utc(self):
        raw = make_raw_news_pandas(n=2)
        result = self.norm.normalize(raw, symbol="AAPL", provider="news")
        assert result["published_at"].dtype == pl.Datetime("us", "UTC")

    def test_empty_headlines_dropped(self):
        raw = make_raw_news_pandas(n=3)
        raw.loc[1, "headline"] = ""       # inject empty headline
        result = self.norm.normalize(raw, symbol="AAPL", provider="news")
        assert len(result) == 2

    def test_empty_dataframe_returns_empty(self):
        result = self.norm.normalize(pd.DataFrame(), symbol="AAPL", provider="news")
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0

    def test_metadata_columns_added(self):
        raw = make_raw_news_pandas(n=2)
        result = self.norm.normalize(raw, symbol="TSLA", provider="newsapi")
        assert result["symbol"].to_list() == ["TSLA"] * 2
        assert result["provider"].to_list() == ["newsapi"] * 2


# ── Fundamentals ──────────────────────────────────────────────────────────────


class TestFundamentalsNormalizer:
    def setup_method(self):
        self.norm = FundamentalsNormalizer()

    def test_basic_normalisation(self):
        raw = make_raw_fundamentals_dict()
        result = self.norm.normalize(raw, symbol="AAPL", provider="yahoo")

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 1

    def test_all_fields_present(self):
        raw = make_raw_fundamentals_dict()
        result = self.norm.normalize(raw, symbol="AAPL", provider="yahoo")

        for col in ("pe_ratio", "pb_ratio", "revenue_growth", "earnings_surprise", "market_cap", "eps"):
            assert col in result.columns

    def test_values_correct(self):
        raw = make_raw_fundamentals_dict()
        result = self.norm.normalize(raw, symbol="AAPL", provider="yahoo")

        assert result["pe_ratio"][0] == pytest.approx(28.4)
        assert result["eps"][0] == pytest.approx(6.42)
        assert result["symbol"][0] == "AAPL"

    def test_missing_fields_are_null(self):
        raw = {"symbol": "AAPL", "as_of": "2024-01-01"}   # minimal dict
        result = self.norm.normalize(raw, symbol="AAPL", provider="yahoo")

        assert result["pe_ratio"][0] is None
        assert result["market_cap"][0] is None


# ── Macro ─────────────────────────────────────────────────────────────────────


class TestMacroNormalizer:
    def setup_method(self):
        self.norm = MacroNormalizer()

    def test_basic_normalisation(self):
        raw = make_raw_macro_pandas(series="CPI", n=12)
        result = self.norm.normalize(raw, series="CPI", provider="fred")

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 12

    def test_series_column_set(self):
        raw = make_raw_macro_pandas(series="FED_FUNDS_RATE", n=6)
        result = self.norm.normalize(raw, series="FED_FUNDS_RATE", provider="fred")
        assert all(v == "FED_FUNDS_RATE" for v in result["series"].to_list())

    def test_sorted_ascending(self):
        raw = make_raw_macro_pandas(series="CPI", n=6)
        result = self.norm.normalize(raw, series="CPI", provider="fred")
        dates = result["date"].to_list()
        assert dates == sorted(dates)

    def test_empty_returns_empty(self):
        result = self.norm.normalize(pd.DataFrame(), series="CPI", provider="fred")
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0
