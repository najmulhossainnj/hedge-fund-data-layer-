from fastapi import APIRouter

from delivery.api.v1 import cache, datasets, fundamentals, health, macro, market, news, ohlcv

router = APIRouter(prefix="/api/v1")

# ── Data endpoints (Research Layer contract — no envelope) ────────────────────
router.include_router(ohlcv.router,         tags=["market data"])
router.include_router(news.router,          tags=["market data"])
router.include_router(fundamentals.router,  tags=["market data"])
router.include_router(macro.router,         tags=["market data"])

# ── Management endpoints (APIResponse envelope) ───────────────────────────────
router.include_router(cache.router,         tags=["cache management"])
router.include_router(datasets.router,      tags=["dataset registry"])
router.include_router(market.router,        tags=["provider management"])
router.include_router(health.router,        tags=["management"])
