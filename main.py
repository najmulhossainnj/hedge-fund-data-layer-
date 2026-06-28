"""
Data Service — main FastAPI application.

Startup sequence (lifespan):
  1. Bootstrap provider registry
  2. Ensure MinIO bucket exists
  3. Log readiness

Shutdown sequence:
  1. Close Redis event stream connection
  2. Dispose SQLAlchemy engine
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from delivery.api.v1.router import router as api_v1_router
from ingestion.providers import registry as provider_registry
from ingestion.storage.parquet_store import ParquetStore
from shared.config import settings
from shared.db.session import engine
from shared.events.streams import event_stream

# ── Logging ───────────────────────────────────────────────────────────────────

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
)

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(message)s",
)

logger = logging.getLogger(__name__)


# ── Lifespan ──────────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("Data Service starting up (env=%s)", settings.APP_ENV)

    # 1. Register all providers
    provider_registry.bootstrap()

    # 2. Ensure MinIO bucket exists
    store = ParquetStore()
    try:
        await store.ensure_bucket()
        logger.info("MinIO bucket '%s' ready", settings.MINIO_BUCKET)
    except Exception as exc:
        logger.warning("MinIO bucket check failed (continuing): %s", exc)

    yield

    # ── Shutdown ──────────────────────────────────────────────────────────
    logger.info("Data Service shutting down")
    await event_stream.close()
    await engine.dispose()


# ── Application ───────────────────────────────────────────────────────────────


app = FastAPI(
    title="Data Service",
    description=(
        "Institutional-grade data ingestion and delivery service. "
        "Acquires, normalises, caches and serves market datasets. "
        "All research engines must consume data exclusively through this service."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(api_v1_router)


# ── Global exception handlers ─────────────────────────────────────────────────


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(Exception)
async def generic_error_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled exception on %s %s", request.method, request.url)
    return JSONResponse(
        status_code=500, content={"detail": "Internal server error."}
    )


# ── Request timing middleware ─────────────────────────────────────────────────


@app.middleware("http")
async def add_timing_header(request: Request, call_next):
    t = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Process-Time"] = f"{time.perf_counter() - t:.4f}s"
    return response


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.APP_HOST,
        port=settings.APP_PORT,
        reload=settings.APP_ENV == "development",
        log_level=settings.LOG_LEVEL.lower(),
    )
