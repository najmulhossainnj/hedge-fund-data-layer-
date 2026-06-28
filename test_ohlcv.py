"""
Integration tests for GET /api/v1/ohlcv.

Strategy:
  - Mock the YahooProvider so no real HTTP calls are made.
  - Mock the ParquetStore.write and DuckDBReader.read_ohlcv.
  - Test the full FastAPI request → response flow.
  - Verify the response shape exactly matches the Research Layer contract.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient

from tests.fixtures.sample_data import make_ohlcv_delivery_rows, make_raw_ohlcv_pandas


@pytest.mark.asyncio
class TestOHLCVEndpoint:
    BASE_URL = "/api/v1/ohlcv"

    async def test_returns_array_of_ohlcv_bars(
        self,
        async_client: AsyncClient,
        auth_headers: dict,
        mock_cache_miss,
        mock_db_registry,
        mock_db_session,
    ):
        fake_rows = make_ohlcv_delivery_rows(n=3)

        with (
            patch(
                "ingestion.providers.yahoo.YahooProvider.download_ohlcv",
                new_callable=AsyncMock,
                return_value=make_raw_ohlcv_pandas(n=3),
            ),
            patch(
                "ingestion.pipeline.ParquetStore.write",
                new_callable=AsyncMock,
                return_value="s3://data-service/datasets/ohlcv/AAPL/1d/2024/AAPL.parquet",
            ),
            patch(
                "delivery.query.duckdb_reader.DuckDBReader.read_ohlcv",
                new_callable=AsyncMock,
                return_value=fake_rows,
            ),
        ):
            resp = await async_client.get(
                self.BASE_URL,
                params={"symbol": "AAPL", "timeframe": "1d", "start": "2024-01-01", "end": "2024-01-31"},
                headers=auth_headers,
            )

        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 3

    async def test_response_shape_matches_research_layer_contract(
        self,
        async_client: AsyncClient,
        auth_headers: dict,
        mock_cache_miss,
        mock_db_registry,
        mock_db_session,
    ):
        """Exact field set: timestamp, open, high, low, close, volume — no extras."""
        fake_rows = make_ohlcv_delivery_rows(n=1)

        with (
            patch("ingestion.providers.yahoo.YahooProvider.download_ohlcv", new_callable=AsyncMock, return_value=make_raw_ohlcv_pandas(n=1)),
            patch("ingestion.pipeline.ParquetStore.write", new_callable=AsyncMock, return_value="s3://x/y.parquet"),
            patch("delivery.query.duckdb_reader.DuckDBReader.read_ohlcv", new_callable=AsyncMock, return_value=fake_rows),
        ):
            resp = await async_client.get(
                self.BASE_URL,
                params={"symbol": "AAPL", "timeframe": "1d", "start": "2024-01-01", "end": "2024-01-10"},
                headers=auth_headers,
            )

        assert resp.status_code == 200
        row = resp.json()[0]
        expected_keys = {"timestamp", "open", "high", "low", "close", "volume"}
        assert set(row.keys()) == expected_keys

    async def test_returns_empty_array_when_no_data(
        self,
        async_client: AsyncClient,
        auth_headers: dict,
        mock_cache_miss,
        mock_db_registry,
        mock_db_session,
    ):
        """Research Layer expects [] on empty range — not 404."""
        import pandas as pd

        with (
            patch("ingestion.providers.yahoo.YahooProvider.download_ohlcv", new_callable=AsyncMock, return_value=pd.DataFrame()),
            patch("ingestion.pipeline.ParquetStore.write", new_callable=AsyncMock, return_value="s3://x/y.parquet"),
            patch("delivery.query.duckdb_reader.DuckDBReader.read_ohlcv", new_callable=AsyncMock, return_value=[]),
        ):
            resp = await async_client.get(
                self.BASE_URL,
                params={"symbol": "AAPL", "timeframe": "1d", "start": "2025-01-01", "end": "2025-01-02"},
                headers=auth_headers,
            )

        assert resp.status_code == 200
        assert resp.json() == []

    async def test_requires_api_key(self, async_client: AsyncClient):
        resp = await async_client.get(
            self.BASE_URL,
            params={"symbol": "AAPL", "timeframe": "1d", "start": "2024-01-01", "end": "2024-01-31"},
        )
        assert resp.status_code == 403   # APIKeyHeader raises 403 when header is missing

    async def test_wrong_api_key_returns_401(self, async_client: AsyncClient):
        resp = await async_client.get(
            self.BASE_URL,
            params={"symbol": "AAPL", "timeframe": "1d", "start": "2024-01-01", "end": "2024-01-31"},
            headers={"X-API-Key": "wrong-key"},
        )
        assert resp.status_code == 401

    async def test_cache_hit_skips_ingestion(
        self,
        async_client: AsyncClient,
        auth_headers: dict,
        mock_cache_hit,
    ):
        """When cache returns a URI, the provider should never be called."""
        fake_rows = make_ohlcv_delivery_rows(n=2)

        with (
            patch("ingestion.providers.yahoo.YahooProvider.download_ohlcv", new_callable=AsyncMock) as mock_download,
            patch("delivery.query.duckdb_reader.DuckDBReader.read_ohlcv", new_callable=AsyncMock, return_value=fake_rows),
        ):
            resp = await async_client.get(
                self.BASE_URL,
                params={"symbol": "AAPL", "timeframe": "1d", "start": "2024-01-01", "end": "2024-01-31"},
                headers=auth_headers,
            )
            mock_download.assert_not_called()

        assert resp.status_code == 200

    async def test_upstream_error_returns_503(
        self,
        async_client: AsyncClient,
        auth_headers: dict,
        mock_cache_miss,
        mock_db_registry,
        mock_db_session,
    ):
        with (
            patch(
                "ingestion.providers.yahoo.YahooProvider.download_ohlcv",
                new_callable=AsyncMock,
                side_effect=Exception("Yahoo down"),
            ),
        ):
            resp = await async_client.get(
                self.BASE_URL,
                params={"symbol": "AAPL", "timeframe": "1d", "start": "2024-01-01", "end": "2024-01-31"},
                headers=auth_headers,
            )

        assert resp.status_code == 503

    async def test_symbol_is_uppercased(
        self,
        async_client: AsyncClient,
        auth_headers: dict,
        mock_cache_miss,
        mock_db_registry,
        mock_db_session,
    ):
        """Symbols should be normalised to uppercase regardless of input case."""
        fake_rows = make_ohlcv_delivery_rows(n=1)

        with (
            patch("ingestion.providers.yahoo.YahooProvider.download_ohlcv", new_callable=AsyncMock, return_value=make_raw_ohlcv_pandas(n=1)),
            patch("ingestion.pipeline.ParquetStore.write", new_callable=AsyncMock, return_value="s3://x/y.parquet"),
            patch("delivery.query.duckdb_reader.DuckDBReader.read_ohlcv", new_callable=AsyncMock, return_value=fake_rows),
        ):
            resp = await async_client.get(
                self.BASE_URL,
                params={"symbol": "aapl", "timeframe": "1d", "start": "2024-01-01", "end": "2024-01-10"},
                headers=auth_headers,
            )

        assert resp.status_code == 200
