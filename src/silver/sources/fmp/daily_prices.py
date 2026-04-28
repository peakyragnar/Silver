"""Parse FMP historical daily-price payloads into Silver rows."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal, InvalidOperation
from math import isfinite
from typing import Any

from silver.prices import DailyPriceRow


PRICE_FIELDS = ("open", "high", "low", "close", "adjClose")
CORE_ROW_FIELDS = frozenset((*PRICE_FIELDS, "date", "volume"))


class FmpDailyPriceParseError(ValueError):
    """Raised when an FMP historical daily-price payload is invalid."""


def parse_historical_daily_prices(payload: Mapping[str, Any]) -> tuple[DailyPriceRow, ...]:
    """Return validated daily price rows sorted by source date.

    This parser only validates and reshapes the vendor payload. It intentionally
    does not infer trading-calendar membership or compute ``available_at``.
    """
    if not isinstance(payload, Mapping):
        raise FmpDailyPriceParseError("payload must be a mapping")

    source_symbol = _required_string(payload, "symbol", "symbol")
    ticker = source_symbol.upper()
    historical = _required(payload, "historical", "historical")
    if not isinstance(historical, list):
        raise FmpDailyPriceParseError("historical must be a list")

    rows: list[DailyPriceRow] = []
    seen_dates: set[date] = set()
    for index, raw_row in enumerate(historical):
        path = f"historical[{index}]"
        if not isinstance(raw_row, Mapping):
            raise FmpDailyPriceParseError(f"{path} must be a mapping")

        source_date = _required_string(raw_row, "date", f"{path}.date")
        row_date = _parse_date(source_date, f"{path}.date")
        if row_date in seen_dates:
            raise FmpDailyPriceParseError(
                f"duplicate date for {ticker}: {source_date}"
            )
        seen_dates.add(row_date)

        rows.append(
            DailyPriceRow(
                ticker=ticker,
                date=row_date,
                open=_positive_decimal(raw_row, "open", f"{path}.open"),
                high=_positive_decimal(raw_row, "high", f"{path}.high"),
                low=_positive_decimal(raw_row, "low", f"{path}.low"),
                close=_positive_decimal(raw_row, "close", f"{path}.close"),
                adj_close=_positive_decimal(raw_row, "adjClose", f"{path}.adjClose"),
                volume=_volume(raw_row, "volume", f"{path}.volume"),
                source="fmp",
                raw_metadata=_raw_metadata(
                    raw_row=raw_row,
                    source_date=source_date,
                    source_symbol=source_symbol,
                ),
            )
        )

    return tuple(sorted(rows, key=lambda row: row.date))


def _required(row: Mapping[str, Any], field: str, path: str) -> Any:
    value = row.get(field)
    if value is None:
        raise FmpDailyPriceParseError(f"{path} is required")
    return value


def _required_string(row: Mapping[str, Any], field: str, path: str) -> str:
    value = _required(row, field, path)
    if not isinstance(value, str) or not value.strip():
        raise FmpDailyPriceParseError(f"{path} must be a non-empty string")
    return value.strip()


def _parse_date(value: str, path: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise FmpDailyPriceParseError(f"{path} must be an ISO date string") from exc


def _positive_decimal(row: Mapping[str, Any], field: str, path: str) -> Decimal:
    value = _decimal(_required(row, field, path), path)
    if value <= 0:
        raise FmpDailyPriceParseError(f"{path} must be positive")
    return value


def _volume(row: Mapping[str, Any], field: str, path: str) -> int:
    value = _decimal(_required(row, field, path), path)
    if value < 0:
        raise FmpDailyPriceParseError(f"{path} must be non-negative")
    if value != value.to_integral_value():
        raise FmpDailyPriceParseError(f"{path} must be an integer")
    return int(value)


def _decimal(value: object, path: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise FmpDailyPriceParseError(f"{path} must be a number")
    if isinstance(value, float) and not isfinite(value):
        raise FmpDailyPriceParseError(f"{path} must be finite")
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise FmpDailyPriceParseError(f"{path} must be a number") from exc
    if not decimal_value.is_finite():
        raise FmpDailyPriceParseError(f"{path} must be finite")
    return decimal_value


def _raw_metadata(
    *,
    raw_row: Mapping[str, Any],
    source_date: str,
    source_symbol: str,
) -> dict[str, Any]:
    metadata = {
        "source_date": source_date,
        "source_symbol": source_symbol,
    }
    for key in sorted(raw_row):
        if key not in CORE_ROW_FIELDS:
            metadata[key] = raw_row[key]
    return metadata
