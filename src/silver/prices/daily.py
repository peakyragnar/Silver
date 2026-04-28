"""Typed daily price rows produced by source parsers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any


@dataclass(frozen=True, slots=True)
class DailyPriceRow:
    """Validated OHLCV row ready for later point-in-time normalization."""

    ticker: str
    date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    adj_close: Decimal
    volume: int
    source: str
    raw_metadata: Mapping[str, Any] = field(default_factory=dict)
