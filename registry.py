"""
Provider registry — maps data_type → provider instance.

New providers are registered here without changing any other core code.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ingestion.providers.base import BaseProvider

logger = logging.getLogger(__name__)

# Lazy imports so providers are only initialised when registered
_registry: dict[str, "BaseProvider"] = {}


def register(provider: "BaseProvider") -> None:
    """Register a provider instance. Last registration for a name wins."""
    _registry[provider.name] = provider
    logger.info("Registered provider: %s", provider.name)


def get(name: str) -> "BaseProvider":
    if name not in _registry:
        raise KeyError(f"Provider '{name}' is not registered. Available: {list(_registry)}")
    return _registry[name]


def get_for_data_type(data_type: str) -> "BaseProvider":
    """
    Return the default provider for a given data type.

    Priority order (first registered wins for each type):
      ohlcv        → yahoo
      news         → news
      fundamentals → yahoo
      macro        → fred
    """
    defaults: dict[str, str] = {
        "ohlcv": "yahoo",
        "news": "news",
        "fundamentals": "yahoo",
        "macro": "fred",
    }
    provider_name = defaults.get(data_type)
    if provider_name is None:
        raise ValueError(f"No default provider mapping for data_type='{data_type}'")
    return get(provider_name)


def all_providers() -> dict[str, "BaseProvider"]:
    return dict(_registry)


def bootstrap() -> None:
    """
    Instantiate and register all built-in providers.
    Called once at application startup from main.py lifespan.
    """
    from ingestion.providers.yahoo import YahooProvider
    from ingestion.providers.news import NewsProvider
    from ingestion.providers.fred import FREDProvider

    register(YahooProvider())
    register(NewsProvider())
    register(FREDProvider())
    logger.info("Provider registry bootstrapped with %d providers", len(_registry))
