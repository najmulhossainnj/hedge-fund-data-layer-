"""
pytest conftest.py — shared fixtures for unit and integration tests.

Rules:
  - Never use real provider APIs.
  - Mock yfinance, fredapi, newsapi with respx / pytest-mock / moto.
  - Integration tests use a real local MinIO (via moto[s3]).
"""

from __future__ import annotations

import asyncio
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from httpx import AsyncClient

from main import app
from shared.config import settings


# ── Event loop ────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def event_loop():
    """Single event loop for the whole test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# ── App client ────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def sync_client() -> TestClient:
    """Synchronous test client (for simple endpoint smoke tests)."""
    return TestClient(app, raise_server_exceptions=True)


@pytest_asyncio.fixture(scope="module")
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    """Async HTTPX client for full async endpoint tests."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client


# ── Auth header ───────────────────────────────────────────────────────────────


@pytest.fixture()
def auth_headers() -> dict[str, str]:
    return {"X-API-Key": settings.DATA_SERVICE_API_KEY}


# ── Cache mock ────────────────────────────────────────────────────────────────


@pytest.fixture()
def mock_cache_miss(monkeypatch):
    """Force every cache lookup to return None (cache miss)."""
    monkeypatch.setattr(
        "delivery.cache.redis_cache.DatasetCache.get",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "delivery.cache.redis_cache.DatasetCache.set",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "ingestion.pipeline.IngestionPipeline._cache_get",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "ingestion.pipeline.IngestionPipeline._cache_set",
        AsyncMock(return_value=None),
    )


@pytest.fixture()
def mock_cache_hit(monkeypatch):
    """Force every cache lookup to return a fake URI."""
    monkeypatch.setattr(
        "ingestion.pipeline.IngestionPipeline._cache_get",
        AsyncMock(return_value="s3://data-service/datasets/ohlcv/AAPL/1d/2024/AAPL_1d_20240101.parquet"),
    )


# ── Event stream mock ─────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def mock_event_stream(monkeypatch):
    """Suppress all event publishing in tests."""
    monkeypatch.setattr(
        "shared.events.streams.EventStream.publish",
        AsyncMock(return_value=None),
    )


# ── DB mock ───────────────────────────────────────────────────────────────────


@pytest.fixture()
def mock_db_registry(monkeypatch):
    """Mock the DatasetRegistry so tests don't need a real Postgres."""
    mock = MagicMock()
    mock.get_by_hash = AsyncMock(return_value=None)
    mock.create = AsyncMock(return_value=MagicMock(id=1))
    mock.log_start = AsyncMock(return_value=MagicMock(id=1))
    mock.log_success = AsyncMock(return_value=None)
    mock.log_failure = AsyncMock(return_value=None)

    monkeypatch.setattr("ingestion.pipeline.DatasetRegistry", lambda: mock)
    monkeypatch.setattr("ingestion.storage.registry.DatasetRegistry", lambda: mock)
    return mock


# ── ParquetStore mock ─────────────────────────────────────────────────────────


@pytest.fixture()
def mock_parquet_store(monkeypatch):
    """Mock ParquetStore.write so tests don't touch MinIO."""
    fake_uri = "s3://data-service/datasets/ohlcv/AAPL/1d/2024/AAPL_1d_20240101.parquet"
    monkeypatch.setattr(
        "ingestion.pipeline.ParquetStore.write",
        AsyncMock(return_value=fake_uri),
    )
    return fake_uri


# ── Session mock ──────────────────────────────────────────────────────────────


@pytest.fixture()
def mock_db_session(monkeypatch):
    """Patch get_session() context manager to yield a MagicMock session."""
    from contextlib import asynccontextmanager

    mock_session = AsyncMock()

    @asynccontextmanager
    async def _fake_get_session():
        yield mock_session

    monkeypatch.setattr("ingestion.pipeline.get_session", _fake_get_session)
    return mock_session
