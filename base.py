"""
BaseNormalizer — every normalizer converts a raw provider DataFrame into a
Polars DataFrame whose schema matches the canonical data model.

Convention (from CONVENTIONS.md):
- Input:  Pandas DataFrame (provider SDKs return Pandas)
- Output: Polars DataFrame (internal pipeline uses Polars)
- Write:  PyArrow via Polars .write_parquet()
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd
import polars as pl


class BaseNormalizer(ABC):
    @abstractmethod
    def normalize(self, df: pd.DataFrame, **kwargs) -> pl.DataFrame:
        """
        Convert a raw provider DataFrame into the canonical Polars schema.

        Must never raise on empty input — return an empty DataFrame with
        the correct schema instead.
        """
