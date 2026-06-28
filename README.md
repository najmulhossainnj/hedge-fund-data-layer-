# Data Service

Institutional-grade data ingestion and delivery service for a quantitative trading platform.

This service is the **single source of truth** for every dataset used in research. The Research Layer, backtesting engines, and ML models never call Yahoo Finance, news APIs, or FRED directly — they call this service.

---

## Architecture

```
Data Providers (Yahoo Finance, NewsAPI, FRED)
              │
              ▼
    Ingestion Service
    ├── Provider Layer       (fetch raw data)
    ├── Normalization Layer  (→ canonical schema)
    ├── Quality Validators   (detect bad data)
    └── Storage Layer        (Parquet on MinIO + metadata in Postgres)
              │
              ▼
    Delivery Service
    ├── Redis Cache          (hash-based, TTL per data type)
    ├── DuckDB Reader        (predicate pushdown on Parquet)
    └── FastAPI Endpoints    (exact Research Layer contract)
              │
              ▼
    Research Layer
    └── market_data_client.py  (the ONLY file that knows this service exists)
```

---

## Quick Start

### Prerequisites
- Docker + Docker Compose
- Python 3.12 (for local development)

### 1. First-time setup

```bash
git clone <repo>
cd data_service

# Copies .env.example → .env, starts all services, runs migrations
make setup
```

### 2. Get API keys (optional but recommended)

| Service | Required | URL |
|---|---|---|
| FRED API | Yes for macro data | https://fred.stlouisfed.org/docs/api/api_key.html |
| NewsAPI | No (RSS fallback) | https://newsapi.org/register |

Edit `.env` and add your keys.

### 3. Verify it's running

```bash
make health
# → {"status": "healthy", "postgres": "ok", "redis": "ok", "minio": "ok"}

make ohlcv
# → JSON array of AAPL daily bars

make fundamentals
# → JSON object with AAPL fundamentals
```

### 4. Pre-warm the cache (optional)

```bash
make prewarm
# Ingests 5 years of daily data for 15 symbols + macro series
# Takes 2-5 minutes on first run, subsequent runs return from cache
```

---

## API Reference

All endpoints require the `X-API-Key` header:
```
X-API-Key: dev-api-key-change-in-production
```

Interactive docs at **http://localhost:8001/docs**

### Data Endpoints (Research Layer contract)

These return raw JSON arrays/objects with **no envelope**.

#### `GET /api/v1/ohlcv`

```bash
curl -H "X-API-Key: $KEY" \
  "http://localhost:8001/api/v1/ohlcv?symbol=AAPL&timeframe=1d&start=2024-01-01&end=2024-12-31"
```

Response:
```json
[
  {"timestamp": "2024-01-02T00:00:00", "open": 185.23, "high": 186.10,
   "low": 184.90, "close": 185.85, "volume": 55234100},
  ...
]
```

Supported timeframes: `1d`, `1w`, `1h`, `1m` (monthly), `30m`, `15m`, `5m`

#### `GET /api/v1/news`

```bash
curl -H "X-API-Key: $KEY" \
  "http://localhost:8001/api/v1/news?symbol=AAPL&start=2024-01-01&end=2024-01-31"
```

Response:
```json
[
  {"headline": "Apple reports record revenue", "published_at": "2024-01-02T14:30:00Z",
   "source": "Reuters", "url": "https://...", "symbol": "AAPL"},
  ...
]
```

#### `GET /api/v1/fundamentals`

```bash
curl -H "X-API-Key: $KEY" \
  "http://localhost:8001/api/v1/fundamentals?symbol=AAPL"
```

Response (single object, not array):
```json
{"symbol": "AAPL", "pe_ratio": 28.4, "pb_ratio": 4.2, "revenue_growth": 0.08,
 "earnings_surprise": 0.03, "market_cap": 2850000000000, "eps": 6.42, "as_of": "2024-01-02"}
```

#### `GET /api/v1/macro`

```bash
curl -H "X-API-Key: $KEY" \
  "http://localhost:8001/api/v1/macro?series=CPI&start=2023-01-01&end=2024-01-01"
```

Supported series: `CPI`, `FED_FUNDS_RATE`, `GDP_GROWTH`, `UNEMPLOYMENT`

Response:
```json
[
  {"date": "2023-01-01", "series": "CPI", "value": 296.8},
  ...
]
```

### Management Endpoints

These return the `APIResponse` envelope: `{status, message, data, metadata, execution_time}`.

| Endpoint | Description |
|---|---|
| `GET /api/v1/health` | Service health (postgres, redis, minio) |
| `GET /api/v1/cache/stats` | Cache key count and memory usage |
| `POST /api/v1/cache/refresh` | Force re-ingestion for a dataset |
| `DELETE /api/v1/cache/invalidate` | Remove a specific hash from cache |
| `DELETE /api/v1/cache/flush` | Flush all cache for a symbol |
| `GET /api/v1/datasets` | List all ingested datasets (paginated) |
| `GET /api/v1/datasets/symbol/{symbol}` | All datasets for a symbol |
| `GET /api/v1/datasets/hash` | Compute hash for given params |
| `GET /api/v1/market/symbols` | Available symbols across all providers |
| `GET /api/v1/market/providers` | Registered providers and health |

---

## Research Layer Integration

Copy `market_data_client.py` to your Research Layer:

```bash
cp market_data_client.py path/to/research/app/engines/feature_engine/market_data_client.py
```

Set these environment variables in the Research Layer:

```bash
DATA_SERVICE_URL=http://localhost:8001
DATA_SERVICE_API_KEY=dev-api-key-change-in-production
```

Usage in any feature plugin:

```python
from app.engines.feature_engine.market_data_client import get_ohlcv, get_news

# Returns pd.DataFrame indexed by timestamp, cols=[open,high,low,close,volume]
df = get_ohlcv("AAPL", timeframe="1d", start="2023-01-01", end="2024-01-01")

# Returns pd.DataFrame with headline, published_at columns
news_df = get_news("AAPL", start="2023-01-01", end="2024-01-01")
```

---

## Development

### Running tests

```bash
make test          # all tests
make test-unit     # fast unit tests only (no infra)
make test-integration  # full endpoint tests (mocked infra)
make test-cov      # with coverage report
```

### Adding a new data provider

1. Create `ingestion/providers/my_provider.py` implementing `BaseProvider`
2. Register it in `ingestion/providers/registry.py` → `bootstrap()`
3. No other files need to change

See `CONVENTIONS.md` for the full plugin guide.

### Makefile commands

```bash
make help          # list all commands
make up            # start all services
make down          # stop all services
make logs          # tail all logs
make migrate       # run pending DB migrations
make db-shell      # open psql
make redis-cli     # open Redis CLI
make generate-key  # generate a secure API key
make clean         # delete all volumes (WARNING: destroys data)
```

---

## Storage Layout

```
MinIO / S3:
  data-service/
    datasets/
      ohlcv/{symbol}/{timeframe}/{year}/{symbol}_{timeframe}_{date}_{hash8}.parquet
      news/{symbol}/{year}/{symbol}_news_{date}_{hash8}.parquet
      fundamentals/{symbol}/{symbol}_fundamentals_{date}_{hash8}.parquet
      macro/{series}/{series}_{year}_{hash8}.parquet

PostgreSQL:
  dataset_records   — one row per ingested file (metadata only)
  ingestion_logs    — audit trail of every ingestion attempt

Redis:
  ds:{sha256_hash}  → s3://bucket/path/to/file.parquet  (TTL per data type)
  sym:{symbol}      → set of hashes (for bulk invalidation)
```

---

## Cache Behaviour

| Data type | TTL | Notes |
|---|---|---|
| OHLCV daily | 24h | Re-ingests once per day |
| OHLCV intraday | 1h | Short TTL for live research |
| News | 6h | New articles appear throughout the day |
| Fundamentals | 24h | Updates after earnings |
| Macro | 72h | FRED data is monthly, rarely changes |

To force a refresh before TTL expiry:
```bash
make cache-stats
curl -X POST -H "X-API-Key: $KEY" \
  "http://localhost:8001/api/v1/cache/refresh?data_type=ohlcv&symbol=AAPL&timeframe=1d&start=2024-01-01&end=2024-12-31"
```

---

## Production Deployment

1. **Generate a strong API key**: `make generate-key`
2. **Use AWS S3** instead of MinIO: set `MINIO_ENDPOINT=s3.amazonaws.com` and `MINIO_SECURE=true`
3. **Scale the app container** horizontally — it is fully stateless
4. **Run migrations separately** as a Kubernetes init container, not on startup
5. **Set `APP_ENV=production`** to disable auto-reload and SQL echo
6. **Use PgBouncer** in front of PostgreSQL under heavy load

---

## Error Reference

| HTTP Code | Meaning |
|---|---|
| 400 | Invalid symbol, unsupported series, or bad date format |
| 401 | Missing or incorrect X-API-Key |
| 403 | Header missing entirely (FastAPI APIKeyHeader behaviour) |
| 404 | Symbol or dataset not found |
| 429 | Upstream provider rate limit hit |
| 503 | Upstream provider unavailable (circuit breaker open) |
