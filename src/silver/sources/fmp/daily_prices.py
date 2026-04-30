"""Parse FMP historical daily-price payloads into Silver rows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal, InvalidOperation
from math import isfinite
from typing import Any

from silver.prices import DailyPriceRow


LEGACY_PRICE_FIELDS = ("open", "high", "low", "close", "adjClose")
STABLE_DIVIDEND_ADJUSTED_PRICE_FIELDS = (
    "adjOpen",
    "adjHigh",
    "adjLow",
    "adjClose",
)
PRICE_FIELDS = (*LEGACY_PRICE_FIELDS, *STABLE_DIVIDEND_ADJUSTED_PRICE_FIELDS)
CORE_ROW_FIELDS = frozenset((*PRICE_FIELDS, "date", "symbol", "volume"))


class FmpDailyPriceParseError(ValueError):
    """Raised when an FMP historical daily-price payload is invalid."""


def parse_historical_daily_prices(payload: object) -> tuple[DailyPriceRow, ...]:
    """Return validated daily price rows sorted by source date.

    This parser only validates and reshapes the vendor payload. It intentionally
    does not infer trading-calendar membership or compute ``available_at``.
    """
    if isinstance(payload, Mapping):
        return _parse_legacy_historical_price_payload(payload)
    if isinstance(payload, list):
        return _parse_stable_dividend_adjusted_payload(payload)
    raise FmpDailyPriceParseError("payload must be a mapping or list")


def _parse_legacy_historical_price_payload(
    payload: Mapping[str, Any],
) -> tuple[DailyPriceRow, ...]:
    source_symbol = _required_string(payload, "symbol", "symbol")
    ticker = source_symbol.upper()
    historical = _required(payload, "historical", "historical")
    if not isinstance(historical, list):
        raise FmpDailyPriceParseError("historical must be a list")

    return _parse_rows(
        historical,
        ticker=ticker,
        source_symbol=source_symbol,
        path_prefix="historical",
        field_names={
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "adj_close": "adjClose",
        },
        require_row_symbol=False,
        metadata_extra={},
    )


def _parse_stable_dividend_adjusted_payload(
    payload: Sequence[object],
) -> tuple[DailyPriceRow, ...]:
    if not payload:
        return ()
    first_row = payload[0]
    if not isinstance(first_row, Mapping):
        raise FmpDailyPriceParseError("rows[0] must be a mapping")
    source_symbol = _required_string(first_row, "symbol", "rows[0].symbol")
    ticker = source_symbol.upper()

    return _parse_rows(
        payload,
        ticker=ticker,
        source_symbol=source_symbol,
        path_prefix="rows",
        field_names={
            "open": "adjOpen",
            "high": "adjHigh",
            "low": "adjLow",
            "close": "adjClose",
            "adj_close": "adjClose",
        },
        require_row_symbol=True,
        metadata_extra={"price_adjustment": "dividend_adjusted"},
    )


def _parse_rows(
    raw_rows: Sequence[object],
    *,
    ticker: str,
    source_symbol: str,
    path_prefix: str,
    field_names: Mapping[str, str],
    require_row_symbol: bool,
    metadata_extra: Mapping[str, Any],
) -> tuple[DailyPriceRow, ...]:
    rows: list[DailyPriceRow] = []
    seen_dates: set[date] = set()
    for index, raw_row in enumerate(raw_rows):
        path = f"{path_prefix}[{index}]"
        if not isinstance(raw_row, Mapping):
            raise FmpDailyPriceParseError(f"{path} must be a mapping")

        if require_row_symbol:
            row_symbol = _required_string(raw_row, "symbol", f"{path}.symbol")
            if row_symbol.upper() != ticker:
                raise FmpDailyPriceParseError(
                    f"{path}.symbol must be {ticker}; got {row_symbol.upper()}"
                )

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
                open=_positive_decimal(
                    raw_row,
                    field_names["open"],
                    f"{path}.{field_names['open']}",
                ),
                high=_positive_decimal(
                    raw_row,
                    field_names["high"],
                    f"{path}.{field_names['high']}",
                ),
                low=_positive_decimal(
                    raw_row,
                    field_names["low"],
                    f"{path}.{field_names['low']}",
                ),
                close=_positive_decimal(
                    raw_row,
                    field_names["close"],
                    f"{path}.{field_names['close']}",
                ),
                adj_close=_positive_decimal(
                    raw_row,
                    field_names["adj_close"],
                    f"{path}.{field_names['adj_close']}",
                ),
                volume=_volume(raw_row, "volume", f"{path}.volume"),
                source="fmp",
                raw_metadata=_raw_metadata(
                    raw_row=raw_row,
                    source_date=source_date,
                    source_symbol=source_symbol,
                    extra=metadata_extra,
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
    extra: Mapping[str, Any],
) -> dict[str, Any]:
    metadata = {
        "source_date": source_date,
        "source_symbol": source_symbol,
    }
    metadata.update(extra)
    for key in sorted(raw_row):
        if key not in CORE_ROW_FIELDS:
            metadata[key] = raw_row[key]
    return metadata
