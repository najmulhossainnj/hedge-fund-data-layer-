.PHONY: help up down build logs shell test test-unit test-integration lint \
        migrate migrate-down db-shell redis-cli minio-console prewarm \
        generate-key clean

# ── Defaults ──────────────────────────────────────────────────────────────────

DATA_SERVICE_URL ?= http://localhost:8001
API_KEY          ?= dev-api-key-change-in-production

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ── Infrastructure ────────────────────────────────────────────────────────────

up: ## Start all services (postgres, redis, minio, data_service)
	docker compose up -d
	@echo "\nData Service:   http://localhost:8001"
	@echo "API Docs:       http://localhost:8001/docs"
	@echo "MinIO Console:  http://localhost:9001  (minioadmin / minioadmin)"
	@echo "Health:         http://localhost:8001/api/v1/health\n"

down: ## Stop all services
	docker compose down

build: ## Rebuild the data_service container
	docker compose build data_service

logs: ## Tail logs from all services
	docker compose logs -f

logs-app: ## Tail logs from data_service only
	docker compose logs -f data_service

shell: ## Open a shell inside the data_service container
	docker compose exec data_service bash

# ── Database ──────────────────────────────────────────────────────────────────

migrate: ## Run pending Alembic migrations
	docker compose exec data_service alembic upgrade head

migrate-down: ## Rollback the last Alembic migration
	docker compose exec data_service alembic downgrade -1

migrate-history: ## Show Alembic migration history
	docker compose exec data_service alembic history

db-shell: ## Open a psql shell
	docker compose exec postgres psql -U user -d data_service

# ── Development tools ─────────────────────────────────────────────────────────

redis-cli: ## Open a Redis CLI
	docker compose exec redis redis-cli

minio-console: ## Open MinIO web console in browser
	@echo "Opening http://localhost:9001 — login: minioadmin / minioadmin"
	@open http://localhost:9001 2>/dev/null || xdg-open http://localhost:9001 2>/dev/null || true

# ── Testing ───────────────────────────────────────────────────────────────────

test: ## Run all tests
	docker compose exec data_service pytest tests/ -v

test-unit: ## Run unit tests only (fast, no infra needed)
	docker compose exec data_service pytest tests/unit/ -v -m "not slow"

test-integration: ## Run integration tests (mocked infra)
	docker compose exec data_service pytest tests/integration/ -v

test-cov: ## Run tests with coverage report
	docker compose exec data_service pytest tests/ --cov=. --cov-report=term-missing

# Local test (without docker — requires deps installed)
test-local: ## Run tests locally (requires: pip install -r requirements.txt)
	pytest tests/ -v

# ── Code quality ──────────────────────────────────────────────────────────────

lint: ## Run ruff linter
	docker compose exec data_service ruff check .

format: ## Auto-format with ruff
	docker compose exec data_service ruff format .

typecheck: ## Run mypy type checker
	docker compose exec data_service mypy . --ignore-missing-imports

# ── Operational ───────────────────────────────────────────────────────────────

prewarm: ## Pre-warm cache for default symbols (requires service running)
	DATA_SERVICE_URL=$(DATA_SERVICE_URL) \
	DATA_SERVICE_API_KEY=$(API_KEY) \
	python scripts/prewarm.py

prewarm-dry: ## Show what prewarm would do without making requests
	python scripts/prewarm.py --dry-run

cache-stats: ## Show cache statistics
	curl -s -H "X-API-Key: $(API_KEY)" $(DATA_SERVICE_URL)/api/v1/cache/stats | python3 -m json.tool

health: ## Check service health
	curl -s -H "X-API-Key: $(API_KEY)" $(DATA_SERVICE_URL)/api/v1/health | python3 -m json.tool

providers: ## List registered providers and health
	curl -s -H "X-API-Key: $(API_KEY)" $(DATA_SERVICE_URL)/api/v1/market/providers | python3 -m json.tool

datasets: ## List ingested datasets
	curl -s -H "X-API-Key: $(API_KEY)" $(DATA_SERVICE_URL)/api/v1/datasets | python3 -m json.tool

# ── Shortcuts for quick manual testing ───────────────────────────────────────

ohlcv: ## Fetch AAPL daily bars (smoke test)
	curl -s -H "X-API-Key: $(API_KEY)" \
	  "$(DATA_SERVICE_URL)/api/v1/ohlcv?symbol=AAPL&timeframe=1d&start=2024-01-01&end=2024-01-10" \
	  | python3 -m json.tool | head -40

news: ## Fetch AAPL news (smoke test)
	curl -s -H "X-API-Key: $(API_KEY)" \
	  "$(DATA_SERVICE_URL)/api/v1/news?symbol=AAPL&start=2024-01-01&end=2024-01-10" \
	  | python3 -m json.tool | head -40

fundamentals: ## Fetch AAPL fundamentals (smoke test)
	curl -s -H "X-API-Key: $(API_KEY)" \
	  "$(DATA_SERVICE_URL)/api/v1/fundamentals?symbol=AAPL" \
	  | python3 -m json.tool

macro: ## Fetch CPI macro data (smoke test)
	curl -s -H "X-API-Key: $(API_KEY)" \
	  "$(DATA_SERVICE_URL)/api/v1/macro?series=CPI&start=2023-01-01&end=2024-01-01" \
	  | python3 -m json.tool | head -30

# ── Setup ─────────────────────────────────────────────────────────────────────

generate-key: ## Generate a secure random API key
	@python3 -c "import secrets; print(secrets.token_hex(32))"

setup: ## First-time setup: copy .env, start services, run migrations
	@[ -f .env ] || cp .env.example .env && echo "Created .env from .env.example — edit it before starting."
	$(MAKE) up
	@sleep 5
	$(MAKE) migrate
	@echo "\nSetup complete. Visit http://localhost:8001/docs to explore the API."

clean: ## Remove all docker volumes (WARNING: deletes all data)
	docker compose down -v
	@echo "All volumes removed."
