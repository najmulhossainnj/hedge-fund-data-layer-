"""
Unit tests for hash computation.

Critical properties:
  1. Deterministic — same inputs always produce the same hash.
  2. Unique     — different inputs produce different hashes.
  3. Stable     — key ordering does not affect the hash.
"""

from __future__ import annotations

import pytest

from ingestion.pipeline import compute_hash


class TestComputeHash:
    def test_deterministic(self):
        params = {"symbol": "AAPL", "timeframe": "1d", "start": "2023-01-01", "end": "2024-01-01"}
        h1 = compute_hash("ohlcv", params)
        h2 = compute_hash("ohlcv", params)
        assert h1 == h2

    def test_different_symbols_produce_different_hashes(self):
        params_aapl = {"provider": "yahoo", "symbol": "AAPL", "timeframe": "1d", "start": "2023-01-01", "end": "2024-01-01"}
        params_msft = {**params_aapl, "symbol": "MSFT"}
        assert compute_hash("ohlcv", params_aapl) != compute_hash("ohlcv", params_msft)

    def test_different_timeframes_produce_different_hashes(self):
        base = {"provider": "yahoo", "symbol": "AAPL", "start": "2023-01-01", "end": "2024-01-01"}
        h_daily = compute_hash("ohlcv", {**base, "timeframe": "1d"})
        h_hourly = compute_hash("ohlcv", {**base, "timeframe": "1h"})
        assert h_daily != h_hourly

    def test_different_date_ranges_produce_different_hashes(self):
        base = {"provider": "yahoo", "symbol": "AAPL", "timeframe": "1d"}
        h1 = compute_hash("ohlcv", {**base, "start": "2023-01-01", "end": "2024-01-01"})
        h2 = compute_hash("ohlcv", {**base, "start": "2022-01-01", "end": "2024-01-01"})
        assert h1 != h2

    def test_different_data_types_produce_different_hashes(self):
        params = {"provider": "yahoo", "symbol": "AAPL"}
        h_ohlcv = compute_hash("ohlcv", params)
        h_fundamentals = compute_hash("fundamentals", params)
        assert h_ohlcv != h_fundamentals

    def test_key_order_does_not_matter(self):
        """Hash must be stable regardless of dict insertion order."""
        params_a = {"provider": "yahoo", "symbol": "AAPL", "timeframe": "1d", "start": "2023-01-01", "end": "2024-01-01"}
        params_b = {"end": "2024-01-01", "start": "2023-01-01", "timeframe": "1d", "symbol": "AAPL", "provider": "yahoo"}
        assert compute_hash("ohlcv", params_a) == compute_hash("ohlcv", params_b)

    def test_hash_is_sha256_length(self):
        """SHA-256 hex digest is 64 characters."""
        h = compute_hash("ohlcv", {"provider": "yahoo", "symbol": "AAPL"})
        assert len(h) == 64

    def test_hash_is_hex_string(self):
        h = compute_hash("ohlcv", {"provider": "yahoo", "symbol": "AAPL"})
        assert all(c in "0123456789abcdef" for c in h)

    def test_different_providers_produce_different_hashes(self):
        base = {"symbol": "AAPL", "timeframe": "1d", "start": "2023-01-01", "end": "2024-01-01"}
        h_yahoo = compute_hash("ohlcv", {**base, "provider": "yahoo"})
        h_polygon = compute_hash("ohlcv", {**base, "provider": "polygon"})
        assert h_yahoo != h_polygon

    def test_macro_hash_uses_series(self):
        base = {"provider": "fred", "start": "2023-01-01", "end": "2024-01-01"}
        h_cpi = compute_hash("macro", {**base, "series": "CPI"})
        h_fed = compute_hash("macro", {**base, "series": "FED_FUNDS_RATE"})
        assert h_cpi != h_fed
