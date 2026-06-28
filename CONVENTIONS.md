# Data Service — Conventions

## The Pandas / Polars Boundary

This is the most important convention in the codebase.
**Both libraries are present but are never used interchangeably.**

| Stage | Library | Reason |
|---|---|---|
| Provider SDK output | **Pandas** | yfinance, fredapi, newsapi all return Pandas DataFrames |
| Ingestion pipeline transforms | **Polars** | Faster, lazy evaluation, no GIL, true parallelism |
| Parquet writes | **Polars** `.write_parquet()` via PyArrow | Best-in-class schema preservation |
| Parquet reads | **DuckDB** | Predicate pushdown directly on S3 files |
| Delivery response | **dict / list** | Converted from DuckDB `.fetchdf()` result |

**Conversion path:**
```
Provider SDK → Pandas DataFrame
                    ↓
            OHLCVNormalizer.normalize()
            pl.from_pandas(df)
                    ↓
            Polars DataFrame (internal)
                    ↓
            df.write_parquet(buf, compression="zstd")
                    ↓
            MinIO / S3 (Parquet file)
                    ↓
            DuckDB read_parquet('s3://...')
                    ↓
            .fetchdf() → list[dict] → JSON response
```

**Never do:**
- `pd.read_parquet()` on stored files — use DuckDB
- Use Polars inside provider SDK calls — the SDKs return Pandas
- Mix Polars and Pandas in the same transformation function


---

## DuckDB Connection Policy

- Create a **new** in-memory DuckDB connection per query
- **Never** share a DuckDB connection across async requests
- Install `httpfs` extension in each new connection (it caches the install on disk)
- Always call `conn.close()` in a `finally` block

```python
# Correct
def _execute_query(sql: str) -> list[dict]:
    conn = duckdb.connect(":memory:")
    try:
        _configure_s3(conn)
        return conn.execute(sql).fetchdf().to_dict(orient="records")
    finally:
        conn.close()

result = await asyncio.to_thread(_execute_query, sql)
```


---

## Storage Rules

1. **Never store datasets in PostgreSQL** — only metadata (URIs, hashes, row counts).
2. **Never overwrite Parquet files** — immutable append-only storage. If a hash collision
   occurs (retroactive corporate action revision), write to a new path with a micro-timestamp suffix.
3. **Compression** — always `zstd`. Better compression ratio than `snappy` for archival datasets.
4. **Partitioning** — always `symbol/timeframe/year/` for OHLCV. Matches the most common query pattern.


---

## Provider Rules

1. **One symbol per call** — never call `yf.download(["AAPL", "MSFT"])`. Always loop.
2. **Wrap all blocking SDK calls** in `asyncio.to_thread()`.
3. **Retry** — max 3 attempts, exponential back-off (tenacity). Apply at the provider level.
4. **Circuit breaker** — threshold of 5 consecutive failures stored in Redis. Raises
   `ProviderUnavailableError` immediately. Auto-resets after 60 seconds.
5. **No provider-specific logic** outside the `ingestion/providers/` package.


---

## API Contract Rules

The delivery endpoints return **raw JSON arrays / objects with no envelope**.

```python
# Correct — raw array
return rows                          # list[dict]

# Wrong — envelope wrapping
return {"status": "success", "data": rows}
```

The Research Layer reads the response directly:
```python
df = pd.DataFrame(response.json())   # expects a plain list
```

**Exception:** Management / admin endpoints (`/health`, future `/admin/*`) use the
`APIResponse` envelope.


---

## Error Handling

- **Data endpoints** — 503 for upstream failures, 400 for invalid params, 404 for symbol not found.
- **News endpoint** — returns `[]` on failure (FinBERT pipeline handles empty input gracefully).
- **Never** raise 500 for upstream provider issues — that is a 503.
- **Always** log the original exception before raising HTTP errors.


---

## Testing Rules

1. **Never call real APIs in tests** — mock yfinance, fredapi, newsapi with `pytest-mock`.
2. **Unit tests** test one component in isolation (normalizer, validator, hash).
3. **Integration tests** test the full FastAPI request → response path with mocked infrastructure.
4. Use `respx` for mocking `httpx` HTTP calls; `pytest-mock` / `unittest.mock` for SDK mocking.
5. Fixture DataFrames live in `tests/fixtures/sample_data.py` — import from there, never redefine inline.


---

## Adding a New Provider

1. Create `ingestion/providers/my_provider.py` implementing `BaseProvider`.
2. Register it in `ingestion/providers/registry.py` → `bootstrap()`.
3. Add a normalizer in `ingestion/normalizers/` if the schema differs.
4. Map it in `delivery/api/v1/` endpoints via a query param (`?provider=my_provider`).
5. **Do not change any existing core code** — that is the point of the plugin architecture.
