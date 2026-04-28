"""Persistence helpers for normalized daily prices."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from silver.prices.daily import DailyPriceRow


DAILY_PRICE_POLICY_NAME = "daily_price"
DEFAULT_DAILY_PRICE_POLICY_VERSION = 1
DEFAULT_NORMALIZATION_VERSION = "fmp_daily_prices_v1"
DEFAULT_PRICE_CURRENCY = "USD"


class DailyPricePersistenceError(ValueError):
    """Raised when a normalized daily price write would violate Silver rules."""


@dataclass(frozen=True, slots=True)
class DailyPriceWriteResult:
    """Summary of a daily-price persistence call."""

    rows_written: int
    tickers: tuple[str, ...]
    dates: tuple[date, ...]
    source: str
    raw_object_id: int
    available_at_policy_id: int
    normalized_by_run_id: int
    normalization_version: str


@dataclass(frozen=True, slots=True)
class DailyPricePolicy:
    """Available-at policy metadata used for daily-price normalization."""

    id: int
    name: str
    version: int
    rule: Mapping[str, Any]


class DailyPriceRepository:
    """Persist parsed daily-price rows into ``silver.prices_daily``.

    The repository intentionally does not fetch source data or commit
    transactions. Callers own raw capture and transaction boundaries.
    """

    def __init__(self, connection: Any):
        self._connection = connection

    def write_daily_prices(
        self,
        rows: Sequence[DailyPriceRow],
        *,
        raw_object_id: int,
        source: str,
        available_at_policy_id: int,
        normalized_by_run_id: int,
        normalization_version: str = DEFAULT_NORMALIZATION_VERSION,
        currency: str = DEFAULT_PRICE_CURRENCY,
    ) -> DailyPriceWriteResult:
        """Persist already-parsed daily-price rows with PIT and raw lineage."""
        normalized_rows = _validated_rows(rows)
        normalized_source = _source(source, "source")
        normalized_raw_object_id = _positive_int(raw_object_id, "raw_object_id")
        normalized_policy_id = _positive_int(
            available_at_policy_id,
            "available_at_policy_id",
        )
        normalized_run_id = _positive_int(
            normalized_by_run_id,
            "normalized_by_run_id",
        )
        normalized_version = _required_label(
            normalization_version,
            "normalization_version",
        )
        normalized_currency = _required_label(currency, "currency").upper()

        if not normalized_rows:
            return DailyPriceWriteResult(
                rows_written=0,
                tickers=(),
                dates=(),
                source=normalized_source,
                raw_object_id=normalized_raw_object_id,
                available_at_policy_id=normalized_policy_id,
                normalized_by_run_id=normalized_run_id,
                normalization_version=normalized_version,
            )

        _validate_row_sources(normalized_rows, normalized_source)
        policy = self._load_daily_price_policy(normalized_policy_id)
        tickers = tuple(sorted({_ticker(row.ticker) for row in normalized_rows}))
        dates = tuple(sorted({row.date for row in normalized_rows}))
        security_ids = self._load_security_ids(tickers)
        self._require_trading_sessions(dates)

        seen_keys: set[tuple[int, date]] = set()
        ordered_rows = sorted(
            normalized_rows,
            key=lambda row: (_ticker(row.ticker), row.date),
        )
        for row in ordered_rows:
            _validate_numeric_row(row)
            ticker = _ticker(row.ticker)
            security_id = security_ids[ticker]
            key = (security_id, row.date)
            if key in seen_keys:
                raise DailyPricePersistenceError(
                    "duplicate daily price row for "
                    f"{ticker} on {row.date.isoformat()}"
                )
            seen_keys.add(key)

            params = {
                "security_id": security_id,
                "date": row.date,
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "adj_close": row.adj_close,
                "volume": row.volume,
                "currency": normalized_currency,
                "source_system": normalized_source,
                "normalization_version": normalized_version,
                "available_at": daily_price_available_at(row.date, policy.rule),
                "available_at_policy_id": policy.id,
                "raw_object_id": normalized_raw_object_id,
                "normalized_by_run_id": normalized_run_id,
            }
            with _cursor(self._connection) as cursor:
                cursor.execute(_UPSERT_DAILY_PRICE_SQL, params)

        return DailyPriceWriteResult(
            rows_written=len(ordered_rows),
            tickers=tickers,
            dates=dates,
            source=normalized_source,
            raw_object_id=normalized_raw_object_id,
            available_at_policy_id=policy.id,
            normalized_by_run_id=normalized_run_id,
            normalization_version=normalized_version,
        )

    def load_daily_price_policy(
        self,
        *,
        version: int = DEFAULT_DAILY_PRICE_POLICY_VERSION,
    ) -> DailyPricePolicy:
        """Load the configured daily-price policy by version."""
        normalized_version = _positive_int(version, "version")
        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_POLICY_BY_NAME_VERSION_SQL,
                {
                    "name": DAILY_PRICE_POLICY_NAME,
                    "version": normalized_version,
                },
            )
            row = cursor.fetchone()
        if row is None:
            raise DailyPricePersistenceError(
                f"{DAILY_PRICE_POLICY_NAME} policy version "
                f"{normalized_version} was not found"
            )
        policy = _policy_record(row)
        _validate_daily_price_rule(policy.rule)
        return policy

    def _load_daily_price_policy(
        self,
        available_at_policy_id: int,
    ) -> DailyPricePolicy:
        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_POLICY_SQL,
                {"available_at_policy_id": available_at_policy_id},
            )
            row = cursor.fetchone()
        if row is None:
            raise DailyPricePersistenceError(
                f"available_at policy {available_at_policy_id} was not found"
            )

        policy = _policy_record(row)
        if policy.name != DAILY_PRICE_POLICY_NAME:
            raise DailyPricePersistenceError(
                f"available_at policy {available_at_policy_id} must be "
                f"{DAILY_PRICE_POLICY_NAME}; got {policy.name}"
            )
        _validate_daily_price_rule(policy.rule)
        return policy

    def _load_security_ids(self, tickers: Sequence[str]) -> dict[str, int]:
        security_ids: dict[str, int] = {}
        for ticker in tickers:
            with _cursor(self._connection) as cursor:
                cursor.execute(_SELECT_SECURITY_SQL, {"ticker": ticker})
                row = cursor.fetchone()
            if row is None:
                raise DailyPricePersistenceError(
                    f"security not found for ticker {ticker}"
                )
            security_ids[ticker] = _row_int(row, "id", 0, "securities.id")
        return security_ids

    def _require_trading_sessions(self, dates: Sequence[date]) -> None:
        with _cursor(self._connection) as cursor:
            cursor.execute(_SELECT_TRADING_SESSIONS_SQL, {"dates": list(dates)})
            rows = cursor.fetchall()

        sessions_by_date: dict[date, bool] = {}
        for row in rows:
            session_date = _row_date(row, "date", 0)
            sessions_by_date[session_date] = _row_bool(row, "is_session", 1)

        for price_date in dates:
            is_session = sessions_by_date.get(price_date)
            if is_session is None:
                raise DailyPricePersistenceError(
                    "trading-calendar row is missing for price date "
                    f"{price_date.isoformat()}"
                )
            if not is_session:
                raise DailyPricePersistenceError(
                    "price date must be a trading session; got "
                    f"{price_date.isoformat()}"
                )


def daily_price_available_at(price_date: date, rule: Mapping[str, Any]) -> datetime:
    """Compute daily-price availability from a validated policy rule."""
    _validate_price_date(price_date)
    _validate_daily_price_rule(rule)
    clock = _rule_time(rule["time"])
    timezone_name = _rule_str(rule, "timezone")
    try:
        policy_timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise DailyPricePersistenceError(
            f"daily_price policy timezone is unknown: {timezone_name}"
        ) from exc
    local_available_at = datetime.combine(price_date, clock, tzinfo=policy_timezone)
    return local_available_at.astimezone(timezone.utc)


def _validated_rows(rows: Sequence[DailyPriceRow]) -> tuple[DailyPriceRow, ...]:
    if isinstance(rows, (str, bytes)) or not isinstance(rows, Sequence):
        raise DailyPricePersistenceError("rows must be a sequence of DailyPriceRow")
    normalized_rows = tuple(rows)
    for index, row in enumerate(normalized_rows, start=1):
        if not isinstance(row, DailyPriceRow):
            raise DailyPricePersistenceError(
                f"rows[{index}] must be a DailyPriceRow"
            )
        _validate_price_date(row.date)
        _ticker(row.ticker)
    return normalized_rows


def _validate_row_sources(rows: Sequence[DailyPriceRow], source: str) -> None:
    for row in rows:
        row_source = _source(row.source, "row.source")
        if row_source != source:
            raise DailyPricePersistenceError(
                f"daily price row source {row_source} does not match "
                f"write source {source}"
            )


def _validate_numeric_row(row: DailyPriceRow) -> None:
    for field_name in ("open", "high", "low", "close", "adj_close"):
        value = getattr(row, field_name)
        if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
            raise DailyPricePersistenceError(
                f"{field_name} must be a positive finite Decimal"
            )
    if isinstance(row.volume, bool) or not isinstance(row.volume, int):
        raise DailyPricePersistenceError("volume must be an integer")
    if row.volume < 0:
        raise DailyPricePersistenceError("volume must be non-negative")


def _validate_daily_price_rule(rule: Mapping[str, Any]) -> None:
    if not isinstance(rule, Mapping):
        raise DailyPricePersistenceError("daily_price policy rule must be a mapping")
    if _rule_str(rule, "type") != "date_at_time":
        raise DailyPricePersistenceError(
            "daily_price policy rule.type must be date_at_time"
        )
    if _rule_str(rule, "base") != "price_date":
        raise DailyPricePersistenceError(
            "daily_price policy rule.base must be price_date"
        )
    _rule_time(rule.get("time"))
    _rule_str(rule, "timezone")


def _validate_price_date(value: object) -> None:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise DailyPricePersistenceError("price date must be a date")


def _rule_str(rule: Mapping[str, Any], key: str) -> str:
    value = rule.get(key)
    if not isinstance(value, str) or not value.strip():
        raise DailyPricePersistenceError(
            f"daily_price policy rule.{key} must be a non-empty string"
        )
    return value.strip()


def _rule_time(value: object) -> time:
    if not isinstance(value, str):
        raise DailyPricePersistenceError(
            "daily_price policy rule.time must be an HH:MM string"
        )
    parts = value.split(":")
    if len(parts) != 2:
        raise DailyPricePersistenceError(
            "daily_price policy rule.time must be an HH:MM string"
        )
    try:
        hour = int(parts[0])
        minute = int(parts[1])
        return time(hour=hour, minute=minute)
    except ValueError as exc:
        raise DailyPricePersistenceError(
            "daily_price policy rule.time must be an HH:MM string"
        ) from exc


def _ticker(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DailyPricePersistenceError("ticker must be a non-empty string")
    return value.strip().upper()


def _source(value: object, name: str) -> str:
    return _required_label(value, name).lower()


def _required_label(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DailyPricePersistenceError(f"{name} must be a non-empty string")
    return value.strip()


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise DailyPricePersistenceError(f"{name} must be a positive integer")
    return value


def _policy_record(row: object) -> DailyPricePolicy:
    rule = _row_value(row, "rule", 3)
    if isinstance(rule, str):
        try:
            rule = json.loads(rule)
        except json.JSONDecodeError as exc:
            raise DailyPricePersistenceError(
                "available_at policy rule must be valid JSON"
            ) from exc
    if not isinstance(rule, Mapping):
        raise DailyPricePersistenceError("available_at policy rule must be a mapping")
    return DailyPricePolicy(
        id=_row_int(row, "id", 0, "available_at_policies.id"),
        name=_row_str(row, "name", 1, "available_at_policies.name"),
        version=_row_int(row, "version", 2, "available_at_policies.version"),
        rule=rule,
    )


def _row_value(row: object, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return row[index]  # type: ignore[index]


def _row_int(row: object, key: str, index: int, name: str) -> int:
    value = _row_value(row, key, index)
    if isinstance(value, bool) or not isinstance(value, int):
        raise DailyPricePersistenceError(
            f"{name} returned by database must be an integer"
        )
    return value


def _row_str(row: object, key: str, index: int, name: str) -> str:
    value = _row_value(row, key, index)
    if not isinstance(value, str) or not value.strip():
        raise DailyPricePersistenceError(
            f"{name} returned by database must be a non-empty string"
        )
    return value.strip()


def _row_date(row: object, key: str, index: int) -> date:
    value = _row_value(row, key, index)
    if isinstance(value, datetime) or not isinstance(value, date):
        raise DailyPricePersistenceError(
            "trading_calendar.date returned by database must be a date"
        )
    return value


def _row_bool(row: object, key: str, index: int) -> bool:
    value = _row_value(row, key, index)
    if not isinstance(value, bool):
        raise DailyPricePersistenceError(
            "trading_calendar.is_session returned by database must be a boolean"
        )
    return value


@contextmanager
def _cursor(connection: Any) -> Any:
    cursor = connection.cursor()
    if hasattr(cursor, "__enter__"):
        with cursor as managed_cursor:
            yield managed_cursor
        return
    try:
        yield cursor
    finally:
        close = getattr(cursor, "close", None)
        if close is not None:
            close()


_SELECT_POLICY_SQL = """
SELECT id, name, version, rule
FROM silver.available_at_policies
WHERE id = %(available_at_policy_id)s
LIMIT 1;
""".strip()

_SELECT_POLICY_BY_NAME_VERSION_SQL = """
SELECT id, name, version, rule
FROM silver.available_at_policies
WHERE name = %(name)s
  AND version = %(version)s
LIMIT 1;
""".strip()

_SELECT_SECURITY_SQL = """
SELECT id
FROM silver.securities
WHERE ticker = %(ticker)s
LIMIT 1;
""".strip()

_SELECT_TRADING_SESSIONS_SQL = """
SELECT date, is_session
FROM silver.trading_calendar
WHERE date = ANY(%(dates)s);
""".strip()

_UPSERT_DAILY_PRICE_SQL = """
INSERT INTO silver.prices_daily (
    security_id,
    date,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    currency,
    source_system,
    normalization_version,
    available_at,
    available_at_policy_id,
    raw_object_id,
    normalized_by_run_id
) VALUES (
    %(security_id)s,
    %(date)s,
    %(open)s,
    %(high)s,
    %(low)s,
    %(close)s,
    %(adj_close)s,
    %(volume)s,
    %(currency)s,
    %(source_system)s,
    %(normalization_version)s,
    %(available_at)s,
    %(available_at_policy_id)s,
    %(raw_object_id)s,
    %(normalized_by_run_id)s
)
ON CONFLICT (security_id, date) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    adj_close = EXCLUDED.adj_close,
    volume = EXCLUDED.volume,
    currency = EXCLUDED.currency,
    source_system = EXCLUDED.source_system,
    normalization_version = EXCLUDED.normalization_version,
    available_at = EXCLUDED.available_at,
    available_at_policy_id = EXCLUDED.available_at_policy_id,
    raw_object_id = EXCLUDED.raw_object_id,
    normalized_by_run_id = EXCLUDED.normalized_by_run_id
WHERE
    silver.prices_daily.open IS DISTINCT FROM EXCLUDED.open
    OR silver.prices_daily.high IS DISTINCT FROM EXCLUDED.high
    OR silver.prices_daily.low IS DISTINCT FROM EXCLUDED.low
    OR silver.prices_daily.close IS DISTINCT FROM EXCLUDED.close
    OR silver.prices_daily.adj_close IS DISTINCT FROM EXCLUDED.adj_close
    OR silver.prices_daily.volume IS DISTINCT FROM EXCLUDED.volume
    OR silver.prices_daily.currency IS DISTINCT FROM EXCLUDED.currency
    OR silver.prices_daily.source_system IS DISTINCT FROM EXCLUDED.source_system
    OR silver.prices_daily.normalization_version IS DISTINCT FROM
        EXCLUDED.normalization_version
    OR silver.prices_daily.available_at IS DISTINCT FROM EXCLUDED.available_at
    OR silver.prices_daily.available_at_policy_id IS DISTINCT FROM
        EXCLUDED.available_at_policy_id
    OR silver.prices_daily.raw_object_id IS DISTINCT FROM EXCLUDED.raw_object_id;
""".strip()
