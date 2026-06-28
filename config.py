from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache
from typing import Optional


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=True, extra="ignore")

    # ── Object Storage (MinIO / S3) ──────────────────────────────────────
    MINIO_ENDPOINT: str = "minio:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_BUCKET: str = "data-service"
    MINIO_SECURE: bool = False

    # ── PostgreSQL ────────────────────────────────────────────────────────
    DATABASE_URL: str = "postgresql+asyncpg://user:pass@postgres:5432/data_service"

    # ── Redis ─────────────────────────────────────────────────────────────
    REDIS_URL: str = "redis://redis:6379/0"

    # ── Provider API Keys ─────────────────────────────────────────────────
    NEWSAPI_KEY: Optional[str] = None        # newsapi.org — optional (RSS fallback)
    FRED_API_KEY: Optional[str] = None       # fred.stlouisfed.org — optional

    # ── Authentication ────────────────────────────────────────────────────
    DATA_SERVICE_API_KEY: str = "dev-api-key-change-in-production"

    # ── Application ───────────────────────────────────────────────────────
    APP_ENV: str = "development"
    LOG_LEVEL: str = "INFO"
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8001

    # ── Circuit Breaker ───────────────────────────────────────────────────
    # After this many consecutive failures, stop calling the provider
    CIRCUIT_BREAKER_THRESHOLD: int = 5
    # Seconds before the circuit resets and allows retries
    CIRCUIT_BREAKER_RESET_TIMEOUT: int = 60

    # ── Cache TTLs (seconds) ──────────────────────────────────────────────
    CACHE_TTL_OHLCV_DAILY: int = 86_400       # 24 h
    CACHE_TTL_OHLCV_INTRADAY: int = 3_600     # 1 h
    CACHE_TTL_NEWS: int = 21_600              # 6 h
    CACHE_TTL_FUNDAMENTALS: int = 86_400      # 24 h
    CACHE_TTL_MACRO: int = 259_200            # 72 h


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
