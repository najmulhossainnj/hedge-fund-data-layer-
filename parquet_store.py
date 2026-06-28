"""
ParquetStore — writes and reads Polars DataFrames as Parquet files in MinIO / S3.

Storage layout (partitioned for query performance):
  datasets/ohlcv/{symbol}/{timeframe}/{year}/{symbol}_{timeframe}_{date}.parquet
  datasets/news/{symbol}/{year}/{symbol}_news_{date}.parquet
  datasets/fundamentals/{symbol}/{symbol}_fundamentals_{as_of_date}.parquet
  datasets/macro/{series}/{series}_{year}.parquet

Design rules:
- Never overwrite existing files. If a path already exists, append a short
  content-hash suffix to create a new immutable version.
- Compression: zstd (better ratio for archival datasets).
- All writes go through asyncio.to_thread (boto3 is blocking).
"""

from __future__ import annotations

import asyncio
import io
import logging
from datetime import datetime, timezone
from typing import Optional

import boto3
import polars as pl
from botocore.exceptions import ClientError

from shared.config import settings

logger = logging.getLogger(__name__)


def _s3_client():
    """Create a boto3 S3 client configured for MinIO or AWS S3."""
    return boto3.client(
        "s3",
        endpoint_url=(
            f"{'https' if settings.MINIO_SECURE else 'http'}://{settings.MINIO_ENDPOINT}"
        ),
        aws_access_key_id=settings.MINIO_ACCESS_KEY,
        aws_secret_access_key=settings.MINIO_SECRET_KEY,
        region_name="us-east-1",    # required by boto3 even for MinIO
    )


def _build_path(
    data_type: str,
    *,
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
    series: Optional[str] = None,
    date_tag: Optional[str] = None,
    hash_suffix: Optional[str] = None,
) -> str:
    """Build the object storage key for a dataset file."""
    suffix = f"_{hash_suffix[:8]}" if hash_suffix else ""
    date_tag = date_tag or datetime.now(timezone.utc).strftime("%Y%m%d")
    year = date_tag[:4]

    if data_type == "ohlcv":
        assert symbol and timeframe
        return (
            f"datasets/ohlcv/{symbol}/{timeframe}/{year}/"
            f"{symbol}_{timeframe}_{date_tag}{suffix}.parquet"
        )
    elif data_type == "news":
        assert symbol
        return (
            f"datasets/news/{symbol}/{year}/"
            f"{symbol}_news_{date_tag}{suffix}.parquet"
        )
    elif data_type == "fundamentals":
        assert symbol
        return (
            f"datasets/fundamentals/{symbol}/"
            f"{symbol}_fundamentals_{date_tag}{suffix}.parquet"
        )
    elif data_type == "macro":
        assert series
        return (
            f"datasets/macro/{series}/"
            f"{series}_{year}{suffix}.parquet"
        )
    else:
        raise ValueError(f"Unknown data_type: {data_type}")


def _s3_uri(key: str) -> str:
    return f"s3://{settings.MINIO_BUCKET}/{key}"


class ParquetStore:
    def __init__(self) -> None:
        self._bucket = settings.MINIO_BUCKET

    async def ensure_bucket(self) -> None:
        """Create the bucket if it does not exist (startup call)."""
        def _sync() -> None:
            client = _s3_client()
            try:
                client.head_bucket(Bucket=self._bucket)
            except ClientError as e:
                if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
                    client.create_bucket(Bucket=self._bucket)
                    logger.info("Created MinIO bucket: %s", self._bucket)
                else:
                    raise
        await asyncio.to_thread(_sync)

    async def write(
        self,
        df: pl.DataFrame,
        data_type: str,
        *,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        series: Optional[str] = None,
        date_tag: Optional[str] = None,
        hash_suffix: Optional[str] = None,
    ) -> str:
        """
        Serialise a Polars DataFrame as Parquet and upload to MinIO.

        Returns the s3:// URI of the written file.
        """
        key = _build_path(
            data_type,
            symbol=symbol,
            timeframe=timeframe,
            series=series,
            date_tag=date_tag,
            hash_suffix=hash_suffix,
        )

        def _sync() -> str:
            client = _s3_client()

            # Check if key already exists — append hash suffix if so
            nonlocal key
            try:
                client.head_object(Bucket=self._bucket, Key=key)
                # File exists — this should not normally happen if hash check is correct,
                # but be defensive. Add timestamp micro-suffix.
                micro = datetime.now(timezone.utc).strftime("%f")
                key = key.replace(".parquet", f"_{micro}.parquet")
                logger.warning("ParquetStore: key collision, writing to %s", key)
            except ClientError as e:
                if e.response["Error"]["Code"] != "404":
                    raise

            # Serialise
            buf = io.BytesIO()
            df.write_parquet(buf, compression="zstd")
            buf.seek(0)

            client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=buf,
                ContentType="application/octet-stream",
            )
            logger.info("ParquetStore: wrote %s (%d rows)", key, len(df))
            return _s3_uri(key)

        return await asyncio.to_thread(_sync)

    async def read(self, uri: str) -> pl.DataFrame:
        """
        Download and deserialise a Parquet file from MinIO.

        Args:
            uri: s3://bucket/key or just the key.
        """
        if uri.startswith("s3://"):
            key = uri[len(f"s3://{self._bucket}/"):]
        else:
            key = uri

        def _sync() -> pl.DataFrame:
            client = _s3_client()
            resp = client.get_object(Bucket=self._bucket, Key=key)
            buf = io.BytesIO(resp["Body"].read())
            return pl.read_parquet(buf)

        return await asyncio.to_thread(_sync)

    async def exists(self, uri: str) -> bool:
        """Check whether a Parquet file exists in MinIO."""
        if uri.startswith("s3://"):
            key = uri[len(f"s3://{self._bucket}/"):]
        else:
            key = uri

        def _sync() -> bool:
            client = _s3_client()
            try:
                client.head_object(Bucket=self._bucket, Key=key)
                return True
            except ClientError:
                return False

        return await asyncio.to_thread(_sync)
