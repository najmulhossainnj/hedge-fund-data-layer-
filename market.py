"""
Symbol discovery endpoints.

GET /api/v1/market/symbols           — list all symbols across registered providers
GET /api/v1/market/symbols/{provider} — list symbols for a specific provider
GET /api/v1/market/providers         — list all registered providers and their health
"""

from __future__ import annotations

import asyncio
import logging
import time

from fastapi import APIRouter, Depends, HTTPException, status

from ingestion.providers import registry as provider_registry
from shared.auth.dependencies import verify_api_key
from shared.models.responses import APIResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/market", dependencies=[Depends(verify_api_key)])


# ── All symbols (union across providers) ──────────────────────────────────────


@router.get("/symbols", response_model=APIResponse, summary="List all available symbols")
async def list_symbols() -> APIResponse:
    """
    Returns the union of symbols available across all registered providers.

    Note: This is a discovery / meta endpoint. The actual symbol universe
    that any given provider can serve is much larger — these are the
    curated defaults exposed via the provider's symbols() method.
    """
    t = time.perf_counter()
    all_providers = provider_registry.all_providers()

    symbol_tasks = {
        name: provider.symbols()
        for name, provider in all_providers.items()
    }

    results: dict[str, list[str]] = {}
    for name, coro in symbol_tasks.items():
        try:
            results[name] = await coro
        except Exception as exc:
            logger.warning("symbols() failed for provider=%s: %s", name, exc)
            results[name] = []

    union = sorted(set(s for syms in results.values() for s in syms))

    return APIResponse(
        status="success",
        message=f"{len(union)} symbols available.",
        data={"symbols": union, "by_provider": results},
        metadata={"provider_count": len(results)},
        execution_time=round(time.perf_counter() - t, 4),
    )


# ── Symbols for a specific provider ──────────────────────────────────────────


@router.get(
    "/symbols/{provider_name}",
    response_model=APIResponse,
    summary="List symbols for a specific provider",
)
async def list_symbols_for_provider(provider_name: str) -> APIResponse:
    """
    Returns the symbol list for a single registered provider.
    """
    t = time.perf_counter()
    try:
        provider = provider_registry.get(provider_name)
    except KeyError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Provider '{provider_name}' is not registered.",
        )

    try:
        symbols = await provider.symbols()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Provider '{provider_name}' failed: {exc}",
        )

    return APIResponse(
        status="success",
        message=f"{len(symbols)} symbols from {provider_name}.",
        data={"provider": provider_name, "symbols": symbols},
        execution_time=round(time.perf_counter() - t, 4),
    )


# ── Provider registry + health ────────────────────────────────────────────────


@router.get("/providers", response_model=APIResponse, summary="List all providers and health")
async def list_providers() -> APIResponse:
    """
    Returns all registered providers with their health status.

    Health checks call each provider's upstream API — this endpoint
    may take a few seconds if upstreams are slow.
    """
    t = time.perf_counter()
    all_providers = provider_registry.all_providers()

    async def _check(name: str, provider) -> dict:
        try:
            healthy = await asyncio.wait_for(provider.health(), timeout=5.0)
        except Exception:
            healthy = False
        return {
            "name":                name,
            "healthy":             healthy,
            "supported_timeframes": getattr(provider, "supported_timeframes", []),
        }

    checks = await asyncio.gather(*[_check(n, p) for n, p in all_providers.items()])

    return APIResponse(
        status="success",
        message=f"{len(checks)} providers registered.",
        data=checks,
        metadata={"healthy": sum(1 for c in checks if c["healthy"])},
        execution_time=round(time.perf_counter() - t, 4),
    )
