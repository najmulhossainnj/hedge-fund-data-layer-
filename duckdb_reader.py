"""
DuckDBReader — reads Parquet files stored on MinIO via DuckDB's httpfs extension.

Why DuckDB here:
  - Predicate pushdown: WHERE clauses are pushed into Parquet row groups, so
    fetching a 1-year slice from a 5-year file reads only the relevant pages.
  - Column pruning: SELECT timestamp, open, high, low, close, volume reads only
    those columns from the Parquet file.
  - No in-memory load of the full file needed.

Connection policy:
  - Create a NEW in-memory DuckDB connection per query.
  - Do NOT share connections across async requests (DuckDB connections are not
    thread-safe when used concurrently).
  - Each connection installs/loads httpfs once, then executes the query.

MinIO configuration:
  - s3_url_style = 'path'  ← required for MinIO (not AWS S3 virtual-host style)
  - s3_use_ssl   = false   ← for local development (set true in prod)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import duckdb
import polars as pl

from shared.config import settings

logger = logging.getLogger(__name__)


def _make_connection() -> duckdb.DuckDBPyConnection:
    """Create and configure a fresh in-memory DuckDB connection for MinIO."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{settings.MINIO_ENDPOINT}';")
    conn.execute(f"SET s3_access_key_id='{settings.MINIO_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{settings.MINIO_SECRET_KEY}';")
    conn.execute(f"SET s3_use_ssl={'true' if settings.MINIO_SECURE else 'false'};")
    conn.execute("SET s3_url_style='path';")  # mandatory for MinIO path-style access
    return conn


class DuckDBReader:
    # ── OHLCV ─────────────────────────────────────────────────────────────

    async def read_ohlcv(
        self,
        uri: str,
        start: str,
        end: str,
    ) -> list[dict[str, Any]]:
        """
        Query OHLCV bars from a Parquet file with date predicate pushdown.

        Returns a list of dicts matching the Research Layer contract:
          [{timestamp, open, high, low, close, volume}, ...]
        """
        sql = f"""
            SELECT
                strftime(timestamp::TIMESTAMPTZ AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S') AS timestamp,
                open,
                high,
                low,
                close,
                volume
            FROM read_parquet('{uri}')
            WHERE timestamp >= '{start}'
              AND timestamp <= '{end}'
            ORDER BY timestamp ASC
        """
        return await asyncio.to_thread(self._execute_to_dicts, sql)

    # ── News ──────────────────────────────────────────────────────────────

    async def read_news(
        self,
        uri: str,
        start: str,
        end: str,
    ) -> list[dict[str, Any]]:
        """
        Query news articles from a Parquet file.

        Returns a list of dicts matching the Research Layer contract:
          [{headline, published_at, source, url, symbol}, ...]
        """
        sql = f"""
            SELECT
                headline,
                strftime(published_at::TIMESTAMPTZ AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS published_at,
                source,
                url,
                symbol
            FROM read_parquet('{uri}')
            WHERE published_at >= '{start}'
              AND published_at <= '{end} 23:59:59'
            ORDER BY published_at ASC
        """
        return await asyncio.to_thread(self._execute_to_dicts, sql)

    # ── Fundamentals ──────────────────────────────────────────────────────

    async def read_fundamentals(self, uri: str) -> Optional[dict[str, Any]]:
        """
        Read fundamentals from a Parquet file.

        Returns the most recent row as a single dict.
        """
        sql = f"""
            SELECT
                symbol,
                pe_ratio,
                pb_ratio,
                revenue_growth,
                earnings_surprise,
                market_cap,
                eps,
                strftime(as_of::TIMESTAMPTZ AT TIME ZONE 'UTC', '%Y-%m-%d') AS as_of
            FROM read_parquet('{uri}')
            ORDER BY as_of DESC
            LIMIT 1
        """
        rows = await asyncio.to_thread(self._execute_to_dicts, sql)
        return rows[0] if rows else None

    # ── Macro ─────────────────────────────────────────────────────────────

    async def read_macro(
        self,
        uri: str,
        series: str,
        start: str,
        end: str,
    ) -> list[dict[str, Any]]:
        """
        Query macro time series from a Parquet file.

        Returns [{date, series, value}, ...]
        """
        sql = f"""
            SELECT
                strftime(date::TIMESTAMPTZ AT TIME ZONE 'UTC', '%Y-%m-%d') AS date,
                series,
                value
            FROM read_parquet('{uri}')
            WHERE series = '{series}'
              AND date >= '{start}'
              AND date <= '{end}'
            ORDER BY date ASC
        """
        return await asyncio.to_thread(self._execute_to_dicts, sql)

    # ── Internal ──────────────────────────────────────────────────────────

    @staticmethod
    def _execute_to_dicts(sql: str) -> list[dict[str, Any]]:
        """Synchronous execution — called via asyncio.to_thread."""
        conn = _make_connection()
        try:
            result = conn.execute(sql).fetchdf()
            # Convert pandas DataFrame to list of dicts; handle NaN → None
            return result.where(result.notna(), other=None).to_dict(orient="records")
        except Exception as exc:
            logger.error("DuckDBReader query failed: %s\nSQL: %s", exc, sql[:300])
            raise
        finally:
            conn.close()


# Module-level singleton
duckdb_reader = DuckDBReader()
