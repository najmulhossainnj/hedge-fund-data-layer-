"""
scripts/prewarm.py — Pre-warm the dataset cache for a list of symbols.

Run this before market hours or before starting the Research Layer to ensure
every commonly-used symbol is already in cache and no Research request
triggers a slow inline ingestion.

Usage:
    python scripts/prewarm.py
    python scripts/prewarm.py --symbols AAPL MSFT GOOGL --timeframes 1d 1h
    python scripts/prewarm.py --symbols AAPL --start 2020-01-01 --end 2025-01-01
    python scripts/prewarm.py --dry-run

Requires the Data Service to be running and DATA_SERVICE_API_KEY to be set.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time
from datetime import datetime, timedelta

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("prewarm")

# ── Defaults ──────────────────────────────────────────────────────────────────

DEFAULT_BASE_URL = os.environ.get("DATA_SERVICE_URL", "http://localhost:8001")
DEFAULT_API_KEY  = os.environ.get("DATA_SERVICE_API_KEY", "dev-api-key-change-in-production")

DEFAULT_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
    "NVDA", "META", "BRK-B", "JPM", "V",
    "SPY", "QQQ", "IWM", "GLD", "TLT",
]

DEFAULT_TIMEFRAMES = ["1d"]

DEFAULT_START = (datetime.now() - timedelta(days=365 * 5)).strftime("%Y-%m-%d")
DEFAULT_END   = datetime.now().strftime("%Y-%m-%d")

DEFAULT_MACRO_SERIES = ["CPI", "FED_FUNDS_RATE", "GDP_GROWTH", "UNEMPLOYMENT"]

# ── Core ──────────────────────────────────────────────────────────────────────


async def prewarm_ohlcv(
    client: httpx.AsyncClient,
    symbol: str,
    timeframe: str,
    start: str,
    end: str,
    dry_run: bool = False,
) -> tuple[bool, float]:
    if dry_run:
        logger.info("[DRY RUN] Would prewarm OHLCV: %s %s %s→%s", symbol, timeframe, start, end)
        return True, 0.0

    t = time.perf_counter()
    try:
        resp = await client.get(
            f"{DEFAULT_BASE_URL}/api/v1/ohlcv",
            params={"symbol": symbol, "timeframe": timeframe, "start": start, "end": end},
        )
        elapsed = time.perf_counter() - t
        if resp.status_code == 200:
            rows = len(resp.json())
            logger.info(
                "✓ OHLCV %s %s — %d rows (%.2fs)", symbol, timeframe, rows, elapsed
            )
            return True, elapsed
        else:
            logger.warning("✗ OHLCV %s %s — HTTP %d (%.2fs)", symbol, timeframe, resp.status_code, elapsed)
            return False, elapsed
    except Exception as exc:
        elapsed = time.perf_counter() - t
        logger.error("✗ OHLCV %s %s — %s (%.2fs)", symbol, timeframe, exc, elapsed)
        return False, elapsed


async def prewarm_news(
    client: httpx.AsyncClient,
    symbol: str,
    start: str,
    end: str,
    dry_run: bool = False,
) -> tuple[bool, float]:
    if dry_run:
        logger.info("[DRY RUN] Would prewarm news: %s %s→%s", symbol, start, end)
        return True, 0.0

    t = time.perf_counter()
    try:
        resp = await client.get(
            f"{DEFAULT_BASE_URL}/api/v1/news",
            params={"symbol": symbol, "start": start, "end": end},
        )
        elapsed = time.perf_counter() - t
        if resp.status_code == 200:
            articles = len(resp.json())
            logger.info("✓ News %s — %d articles (%.2fs)", symbol, articles, elapsed)
            return True, elapsed
        else:
            logger.warning("✗ News %s — HTTP %d (%.2fs)", symbol, resp.status_code, elapsed)
            return False, elapsed
    except Exception as exc:
        elapsed = time.perf_counter() - t
        logger.error("✗ News %s — %s (%.2fs)", symbol, exc, elapsed)
        return False, elapsed


async def prewarm_macro(
    client: httpx.AsyncClient,
    series: str,
    start: str,
    end: str,
    dry_run: bool = False,
) -> tuple[bool, float]:
    if dry_run:
        logger.info("[DRY RUN] Would prewarm macro: %s %s→%s", series, start, end)
        return True, 0.0

    t = time.perf_counter()
    try:
        resp = await client.get(
            f"{DEFAULT_BASE_URL}/api/v1/macro",
            params={"series": series, "start": start, "end": end},
        )
        elapsed = time.perf_counter() - t
        if resp.status_code == 200:
            rows = len(resp.json())
            logger.info("✓ Macro %s — %d points (%.2fs)", series, rows, elapsed)
            return True, elapsed
        else:
            logger.warning("✗ Macro %s — HTTP %d (%.2fs)", series, resp.status_code, elapsed)
            return False, elapsed
    except Exception as exc:
        elapsed = time.perf_counter() - t
        logger.error("✗ Macro %s — %s (%.2fs)", series, exc, elapsed)
        return False, elapsed


async def run(args: argparse.Namespace) -> None:
    headers = {
        "X-API-Key": DEFAULT_API_KEY,
        "Accept": "application/json",
    }

    # Concurrency limiter — don't hammer the service with 100 parallel requests
    semaphore = asyncio.Semaphore(args.concurrency)

    async def bounded(coro):
        async with semaphore:
            return await coro

    t_total = time.perf_counter()
    success = 0
    failed = 0

    async with httpx.AsyncClient(headers=headers, timeout=120.0) as client:
        tasks = []

        # OHLCV tasks
        for symbol in args.symbols:
            for timeframe in args.timeframes:
                tasks.append(
                    bounded(prewarm_ohlcv(client, symbol, timeframe, args.start, args.end, args.dry_run))
                )

        # News tasks
        if not args.skip_news:
            for symbol in args.symbols:
                tasks.append(
                    bounded(prewarm_news(client, symbol, args.start, args.end, args.dry_run))
                )

        # Macro tasks
        if not args.skip_macro:
            for series in DEFAULT_MACRO_SERIES:
                tasks.append(
                    bounded(prewarm_macro(client, series, args.start, args.end, args.dry_run))
                )

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                failed += 1
            elif isinstance(r, tuple):
                ok, _ = r
                if ok:
                    success += 1
                else:
                    failed += 1

    total_elapsed = time.perf_counter() - t_total
    logger.info(
        "\nPre-warm complete: %d succeeded, %d failed, %.1fs total",
        success, failed, total_elapsed,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Pre-warm the Data Service cache.")
    parser.add_argument("--symbols",     nargs="+",  default=DEFAULT_SYMBOLS)
    parser.add_argument("--timeframes",  nargs="+",  default=DEFAULT_TIMEFRAMES)
    parser.add_argument("--start",       default=DEFAULT_START)
    parser.add_argument("--end",         default=DEFAULT_END)
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--skip-news",   action="store_true")
    parser.add_argument("--skip-macro",  action="store_true")
    parser.add_argument("--dry-run",     action="store_true",
                        help="Print what would be fetched without making requests")
    args = parser.parse_args()

    logger.info("Pre-warming %d symbols × %d timeframes", len(args.symbols), len(args.timeframes))
    logger.info("Date range: %s → %s", args.start, args.end)
    logger.info("Concurrency: %d parallel requests", args.concurrency)

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
