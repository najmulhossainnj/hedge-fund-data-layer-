FROM python:3.12-slim

WORKDIR /app

# System dependencies for TA-Lib / C extensions (pandas, pyarrow, duckdb)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Run database migrations then start the server
# Migrations are applied on container startup to keep things simple for v1.
# In production, run migrations as a separate init container.
CMD ["sh", "-c", "alembic upgrade head && uvicorn main:app --host 0.0.0.0 --port 8001 --workers 2"]

EXPOSE 8001

# Health check — relies on the /api/v1/health endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8001/api/v1/health || exit 1
