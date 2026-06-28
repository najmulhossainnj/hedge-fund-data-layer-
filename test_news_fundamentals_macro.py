"""
Integration tests for:
  GET /api/v1/news
  GET /api/v1/fundamentals
  GET /api/v1/macro
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
from httpx import AsyncClient

from tests.fixtures.sample_data import (
    make_raw_fundamentals_dict,
    make_raw_macro_pandas,
    make_raw_news_pandas,
)


# ── News ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
class TestNewsEndpoint:
    BASE_URL = "/api/v1/news"

    async def test_returns_array_of_articles(
        self,
        async_client: AsyncClient,
        auth_headers: dict,
        mock_cache_miss,
        mock_db_registry,
        mock_db_session,
    ):
        fake_rows = [
            {
                "headline": "Apple reports record revenue",
                "published_at": "2024-01-02T14:30:00Z",
                "source": "Reuters",
                "url": "https://reuters.com/article/1",
                "symbol": "AAPL",
            }
        ]

        with (
            patch("ingestion.providers.news.NewsProvider.download_news", new_callable=AsyncMock, return_value=make_raw_news_pandas(n=1)),
            patch("ingestion.pipeline.ParquetStore.write", new_callable=AsyncMock, return_value="s3://x/news.parquet"),
            patch("delivery.query.duckdb_reader.DuckDBReader.read_news", new_callable=AsyncMock, return_value=fake_rows),
        ):
            resp = await async_client.get(
                self.BASE_URL,
                params={"symbol": "AAPL", "start": "2024-01-01", "end": "2024-01-31"},
                headers=auth_headers,
            )

        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert "headline" in data[0]
        assert "published_at" in data[0]

    async def test_published_at_has_timezone(
        self,
        async_client: AsyncClient,
        auth_headers: dict,
        mock_cache_miss,
        mock_db_registry,
        mock_db_session,
    ):
        """published_at must include a timezone marker — Research Layer uses utc=True."""
        fake_rows = [
            {"headline": "News", "published_at": "2024-01-02T14:30:00Z", "source": None, "url": None, "symbol": "AAPL"}
        ]

        with (
            patch("ingestion.providers.news.NewsProvider.download_news", new_callable=AsyncMock, return_value=make_raw_news_pandas(n=1)),
            patch("ingestion.pipeline.ParquetStore.write", new_callable=AsyncMock, return_value="s3://x/news.parquet"),
            patch("delivery.query.duckdb_reader.DuckDBReader.read_news", new_callable=AsyncMock, return_value=fake_rows),
        ):
            resp = await async_client.get(
                self.BASE_URL,
                params={"symbol": "AAPL", "start": "2024-01-01", "end": "2024-01-31"},
                headers=auth_headers,
            )

        row = resp.json()[0]
        assert row["published_at"].endswith("Z") or "+" in row["published_at"]

    async def test_returns_empty_array_on_no_news(
        self,
        async_client: AsyncClient,
        auth_headers: dict,
        mock_cache_miss,
        mock_db_registry,
        mock_db_session,
    ):
        with (
            patch("ingestion.providers.news.NewsProvider.download_news", new_callable=AsyncMock, return_value=pd.DataFrame()),
            patch("ingestion.pipeline.ParquetStore.write", new_callable=AsyncMock, return_value="s3://x/news.parquet"),
            patch("delivery.query.duckdb_reader.DuckDBReader.read_news", new_callable=AsyncMock, return_value=[]),
        ):
            resp = await async_client.get(
                self.BASE_URL,
                params={"symbol": "AAPL", "start": "2024-01-01", "end": "2024-01-31"},
                headers=auth_headers,
            )

        assert resp.status_code == 200
        assert resp.json() == []

    async def test_provider_error_returns_empty_array(
        self,
        async_client: AsyncClient,
        auth_headers: dict,
        mock_cache_miss,
        mock_db_registry,
        mock_db_session,
    ):
        """News errors return [] rather than 503 — sentiment pipeline handles empty gracefully."""
        with patch(
            "ingestion.providers.news.NewsProvider.download_news",
            new_callable=AsyncMock,
            side_effect=Exception("RSS unavailable"),
        ):
            resp = await async_client.get(
                self.BASE_URL,
                params={"symbol": "AAPL", "start": "2024-01-01", "end": "2024-01-31"},
                headers=auth_headers,
            )

        assert resp.status_code == 200
        assert resp.json() == []

    async def test_requires_api_key(self, async_client: AsyncClient):
        resp = await async_client.get(
            self.BASE_URL,
            params={"symbol": "AAPL", "start": "2024-01-01", "end": "2024-01-31"},
        )
        assert resp.status_code == 403


# ── Fundamentals ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
class TestFundamentalsEndpoint:
    BASE_URL = "/api/v1/fundamentals"

    async def test_returns_single_object(
        self,
        async_client: AsyncClient,
        auth_headers: dict,
        mock_cache_miss,
        mock_db_registry,
        mock_db_session,
    ):
        fake_row = {
            "symbol": "AAPL",
            "pe_ratio": 28.4,
            "pb_ratio": 4.2,
            "revenue_growth": 0.08,
            "earnings_surprise": 0.03,
            "market_cap": 2_850_000_000_000.0,
            "eps": 6.42,
            "as_of": "2024-01-02",
        }

        with (
            patch("ingestion.providers.yahoo.YahooProvider.download_fundamentals", new_callable=AsyncMock, return_value=make_raw_fundamentals_dict()),
            patch("ingestion.pipeline.ParquetStore.write", new_callable=AsyncMock, return_value="s3://x/fund.parquet"),
            patch("delivery.query.duckdb_reader.DuckDBReader.read_fundamentals", new_callable=AsyncMock, return_value=fake_row),
        ):
            resp = await async_client.get(
                self.BASE_URL,
                params={"symbol": "AAPL"},
                headers=auth_headers,
            )

        assert resp.status_code == 200
        data = resp.json()
        # Must be a dict (object), NOT a list
        assert isinstance(data, dict)
        assert data["symbol"] == "AAPL"

    async def test_response_fields(
        self,
        async_client: AsyncClient,
        auth_headers: dict,
        mock_cache_miss,
        mock_db_registry,
        mock_db_session,
    ):
        fake_row = {"symbol": "AAPL", "pe_ratio": 28.4, "pb_ratio": None, "revenue_growth": 0.08,
                    "earnings_surprise": None, "market_cap": 2.85e12, "eps": 6.42, "as_of": "2024-01-02"}

        with (
            patch("ingestion.providers.yahoo.YahooProvider.download_fundamentals", new_callable=AsyncMock, return_value=make_raw_fundamentals_dict()),
            patch("ingestion.pipeline.ParquetStore.write", new_callable=AsyncMock, return_value="s3://x/fund.parquet"),
            patch("delivery.query.duckdb_reader.DuckDBReader.read_fundamentals", new_callable=AsyncMock, return_value=fake_row),
        ):
            resp = await async_client.get(self.BASE_URL, params={"symbol": "AAPL"}, headers=auth_headers)

        data = resp.json()
        assert "symbol" in data
        assert "as_of" in data
        assert "pe_ratio" in data

    async def test_requires_api_key(self, async_client: AsyncClient):
        resp = await async_client.get(self.BASE_URL, params={"symbol": "AAPL"})
        assert resp.status_code == 403


# ── Macro ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
class TestMacroEndpoint:
    BASE_URL = "/api/v1/macro"

    async def test_returns_array_of_macro_points(
        self,
        async_client: AsyncClient,
        auth_headers: dict,
        mock_cache_miss,
        mock_db_registry,
        mock_db_session,
    ):
        fake_rows = [
            {"date": "2023-01-01", "series": "CPI", "value": 296.8},
            {"date": "2023-02-01", "series": "CPI", "value": 298.1},
        ]

        with (
            patch("ingestion.providers.fred.FREDProvider.download_macro", new_callable=AsyncMock, return_value=make_raw_macro_pandas(n=2)),
            patch("ingestion.pipeline.ParquetStore.write", new_callable=AsyncMock, return_value="s3://x/macro.parquet"),
            patch("delivery.query.duckdb_reader.DuckDBReader.read_macro", new_callable=AsyncMock, return_value=fake_rows),
        ):
            resp = await async_client.get(
                self.BASE_URL,
                params={"series": "CPI", "start": "2023-01-01", "end": "2023-12-31"},
                headers=auth_headers,
            )

        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert "date" in data[0]
        assert "series" in data[0]
        assert "value" in data[0]

    async def test_unsupported_series_returns_400(
        self,
        async_client: AsyncClient,
        auth_headers: dict,
    ):
        resp = await async_client.get(
            self.BASE_URL,
            params={"series": "INVALID_SERIES", "start": "2023-01-01", "end": "2023-12-31"},
            headers=auth_headers,
        )
        assert resp.status_code == 400

    async def test_all_supported_series_accepted(
        self,
        async_client: AsyncClient,
        auth_headers: dict,
        mock_cache_miss,
        mock_db_registry,
        mock_db_session,
    ):
        supported = ["CPI", "FED_FUNDS_RATE", "GDP_GROWTH", "UNEMPLOYMENT"]

        for series in supported:
            with (
                patch("ingestion.providers.fred.FREDProvider.download_macro", new_callable=AsyncMock, return_value=make_raw_macro_pandas(series=series, n=1)),
                patch("ingestion.pipeline.ParquetStore.write", new_callable=AsyncMock, return_value="s3://x/macro.parquet"),
                patch("delivery.query.duckdb_reader.DuckDBReader.read_macro", new_callable=AsyncMock, return_value=[]),
            ):
                resp = await async_client.get(
                    self.BASE_URL,
                    params={"series": series, "start": "2023-01-01", "end": "2023-12-31"},
                    headers=auth_headers,
                )
            assert resp.status_code == 200, f"Failed for series={series}"

    async def test_requires_api_key(self, async_client: AsyncClient):
        resp = await async_client.get(
            self.BASE_URL,
            params={"series": "CPI", "start": "2023-01-01", "end": "2023-12-31"},
        )
        assert resp.status_code == 403
