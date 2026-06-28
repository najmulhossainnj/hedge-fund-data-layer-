"""
Dataset management endpoints.

GET /api/v1/datasets               — list all ingested datasets (paginated)
GET /api/v1/datasets/{hash}        — get metadata for a specific dataset by hash
GET /api/v1/datasets/symbol/{sym}  — list all datasets for a symbol
GET /api/v1/datasets/versions      — list all versions of a dataset (by params)

These query the PostgreSQL DatasetRegistry — the metadata layer only.
They never read from MinIO.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select

from ingestion.pipeline import compute_hash
from ingestion.storage.registry import DatasetRegistry
from shared.auth.dependencies import verify_api_key
from shared.db.models import DatasetRecord
from shared.db.session import get_db
from shared.models.responses import APIResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/datasets", dependencies=[Depends(verify_api_key)])
_registry = DatasetRegistry()


def _record_to_dict(r: DatasetRecord) -> dict:
    return {
        "id":             r.id,
        "provider":       r.provider,
        "data_type":      r.data_type,
        "symbol":         r.symbol,
        "timeframe":      r.timeframe,
        "series":         r.series,
        "start_date":     r.start_date,
        "end_date":       r.end_date,
        "rows":           r.rows,
        "columns":        r.columns,
        "storage_uri":    r.storage_uri,
        "hash":           r.hash,
        "quality_passed": r.quality_passed,
        "quality_issues": r.quality_issues,
        "created_at":     r.created_at.isoformat() if r.created_at else None,
    }


# ── List all datasets ─────────────────────────────────────────────────────────


@router.get("", response_model=APIResponse, summary="List all ingested datasets")
async def list_datasets(
    data_type: Optional[str] = Query(None, description="Filter by type: ohlcv | news | fundamentals | macro"),
    provider: Optional[str] = Query(None, description="Filter by provider: yahoo | news | fred"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db=Depends(get_db),
) -> APIResponse:
    """
    Returns a paginated list of all datasets in the registry.

    Only metadata is returned — no actual data from MinIO.
    Use the storage_uri field to know where the Parquet file lives.
    """
    t = time.perf_counter()

    stmt = select(DatasetRecord).order_by(DatasetRecord.created_at.desc())
    if data_type:
        stmt = stmt.where(DatasetRecord.data_type == data_type)
    if provider:
        stmt = stmt.where(DatasetRecord.provider == provider)
    stmt = stmt.offset(offset).limit(limit)

    result = await db.execute(stmt)
    records = result.scalars().all()

    return APIResponse(
        status="success",
        message=f"Found {len(records)} datasets.",
        data=[_record_to_dict(r) for r in records],
        metadata={"limit": limit, "offset": offset, "returned": len(records)},
        execution_time=round(time.perf_counter() - t, 4),
    )


# ── Get by hash ───────────────────────────────────────────────────────────────


@router.get("/by-hash/{hash_val}", response_model=APIResponse, summary="Get dataset by hash")
async def get_dataset_by_hash(
    hash_val: str,
    db=Depends(get_db),
) -> APIResponse:
    """
    Returns metadata for the dataset identified by its SHA-256 hash.

    The hash is deterministic: same provider + symbol + timeframe + dates
    always produce the same hash, so you can reconstruct it without querying.
    """
    t = time.perf_counter()
    record = await _registry.get_by_hash(db, hash_val)

    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No dataset found with hash={hash_val[:12]}...",
        )

    return APIResponse(
        status="success",
        message="Dataset found.",
        data=_record_to_dict(record),
        execution_time=round(time.perf_counter() - t, 4),
    )


# ── List by symbol ────────────────────────────────────────────────────────────


@router.get("/symbol/{symbol}", response_model=APIResponse, summary="List datasets for a symbol")
async def list_datasets_for_symbol(
    symbol: str,
    data_type: Optional[str] = Query(None),
    db=Depends(get_db),
) -> APIResponse:
    """
    Returns all ingested datasets for a given symbol, newest first.

    Useful for auditing what date ranges are available before building
    a feature matrix in the Research Layer.
    """
    t = time.perf_counter()
    symbol = symbol.upper()
    records = await _registry.list_by_symbol(db, symbol, data_type=data_type)

    return APIResponse(
        status="success",
        message=f"Found {len(records)} datasets for {symbol}.",
        data=[_record_to_dict(r) for r in records],
        metadata={"symbol": symbol, "data_type": data_type},
        execution_time=round(time.perf_counter() - t, 4),
    )


# ── Compute hash (utility) ────────────────────────────────────────────────────


@router.get("/hash", response_model=APIResponse, summary="Compute dataset hash from params")
async def compute_dataset_hash(
    data_type: str = Query(...),
    symbol: Optional[str] = Query(None),
    timeframe: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    series: Optional[str] = Query(None),
    provider: str = Query("yahoo"),
) -> APIResponse:
    """
    Computes the SHA-256 hash that would be used to identify a dataset
    with the given parameters.

    Use this to:
    - Check the cache before requesting data
    - Construct a /cache/refresh call
    - Verify determinism
    """
    t = time.perf_counter()

    params: dict = {"provider": provider}
    if symbol:
        params["symbol"] = symbol.upper()
    if timeframe:
        params["timeframe"] = timeframe
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if series:
        params["series"] = series.upper()

    hash_val = compute_hash(data_type, params)

    return APIResponse(
        status="success",
        message="Hash computed.",
        data={"hash": hash_val, "data_type": data_type, "params": params},
        execution_time=round(time.perf_counter() - t, 4),
    )
