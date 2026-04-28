"""Persistence helpers for materialized forward-return labels."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from silver.prices.daily import DailyPriceRow
from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


class ForwardLabelPersistenceError(ValueError):
    """Raised when forward-label persistence inputs violate Silver rules."""


@dataclass(frozen=True, slots=True)
class ForwardLabelPriceObservation:
    """Normalized adjusted-close price input for label materialization."""

    security_id: int
    row: DailyPriceRow
    available_at: datetime
    available_at_policy_id: int


@dataclass(frozen=True, slots=True)
class ForwardLabelRecord:
    """Database-ready forward-return label row."""

    security_id: int
    label_date: date
    horizon_days: int
    horizon_date: date
    horizon_close_at: datetime
    label_version: int
    start_adj_close: Decimal
    end_adj_close: Decimal
    realized_raw_return: Decimal
    benchmark_security_id: int | None
    realized_excess_return: Decimal | None
    available_at: datetime
    available_at_policy_id: int
    computed_by_run_id: int
    metadata: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class ForwardLabelWriteResult:
    """Summary of a forward-label write call."""

    records_seen: int
    rows_changed: int
    label_version: int | None


class ForwardLabelRepository:
    """Read label inputs and write ``silver.forward_return_labels`` rows.

    The repository intentionally does not commit transactions. Callers own
    transaction boundaries so run metadata and label rows can commit together.
    """

    def __init__(self, connection: Any):
        self._connection = connection

    def load_trading_calendar(self) -> TradingCalendar:
        """Load all seeded trading-calendar rows for horizon arithmetic."""
        with _cursor(self._connection) as cursor:
            cursor.execute(_SELECT_TRADING_CALENDAR_SQL, {})
            rows = cursor.fetchall()
        return TradingCalendar(tuple(_calendar_row(row) for row in rows))

    def load_universe_price_observations(
        self,
        *,
        universe_name: str,
        label_start_date: date | None = None,
        label_end_date: date | None = None,
        price_end_date: date | None = None,
    ) -> tuple[ForwardLabelPriceObservation, ...]:
        """Load normalized prices for securities overlapping a label window."""
        normalized_universe = _non_empty_str(universe_name, "universe_name")
        _date_or_none(label_start_date, "label_start_date")
        _date_or_none(label_end_date, "label_end_date")
        _date_or_none(price_end_date, "price_end_date")

        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_UNIVERSE_PRICES_SQL,
                {
                    "universe_name": normalized_universe,
                    "label_start_date": label_start_date,
                    "label_end_date": label_end_date,
                    "price_start_date": label_start_date,
                    "price_end_date": price_end_date,
                },
            )
            rows = cursor.fetchall()
        return tuple(_price_observation(row) for row in rows)

    def load_label_dates_by_security(
        self,
        *,
        universe_name: str,
        label_start_date: date | None = None,
        label_end_date: date | None = None,
    ) -> dict[int, tuple[date, ...]]:
        """Load PIT-eligible label dates with an as-of price for each security."""
        normalized_universe = _non_empty_str(universe_name, "universe_name")
        _date_or_none(label_start_date, "label_start_date")
        _date_or_none(label_end_date, "label_end_date")

        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_LABEL_DATES_SQL,
                {
                    "universe_name": normalized_universe,
                    "label_start_date": label_start_date,
                    "label_end_date": label_end_date,
                },
            )
            rows = cursor.fetchall()

        dates_by_security: dict[int, list[date]] = {}
        for row in rows:
            security_id = _row_int(row, "security_id", 0, "security_id")
            label_date = _row_date(row, "date", 1, "prices_daily.date")
            dates_by_security.setdefault(security_id, []).append(label_date)
        return {
            security_id: tuple(sorted(set(label_dates)))
            for security_id, label_dates in dates_by_security.items()
        }

    def load_available_at_policy_versions(
        self,
        policy_ids: Sequence[int],
    ) -> dict[str, int]:
        """Load policy versions for analytics-run reproducibility metadata."""
        normalized_policy_ids = sorted(
            {_positive_int(policy_id, "available_at_policy_id") for policy_id in policy_ids}
        )
        if not normalized_policy_ids:
            return {}

        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_POLICY_VERSIONS_SQL,
                {"available_at_policy_ids": normalized_policy_ids},
            )
            rows = cursor.fetchall()

        versions: dict[str, int] = {}
        seen_ids: set[int] = set()
        for row in rows:
            policy_id = _row_int(row, "id", 0, "available_at_policies.id")
            name = _row_str(row, "name", 1, "available_at_policies.name")
            version = _row_int(row, "version", 2, "available_at_policies.version")
            seen_ids.add(policy_id)
            existing = versions.get(name)
            if existing is not None and existing != version:
                raise ForwardLabelPersistenceError(
                    f"conflicting available_at policy versions for {name}"
                )
            versions[name] = version

        missing = set(normalized_policy_ids) - seen_ids
        if missing:
            missing_ids = ", ".join(str(policy_id) for policy_id in sorted(missing))
            raise ForwardLabelPersistenceError(
                f"available_at policy id(s) not found: {missing_ids}"
            )
        return dict(sorted(versions.items()))

    def create_label_generation_run(
        self,
        *,
        code_git_sha: str,
        available_at_policy_versions: Mapping[str, int],
        parameters: Mapping[str, Any],
        input_fingerprints: Mapping[str, Any],
    ) -> int:
        """Create an ``analytics_runs`` row for a label-generation job."""
        params = {
            "code_git_sha": _non_empty_str(code_git_sha, "code_git_sha"),
            "available_at_policy_versions": _stable_json(
                available_at_policy_versions,
                "available_at_policy_versions",
            ),
            "parameters": _stable_json(parameters, "parameters"),
            "input_fingerprints": _stable_json(
                input_fingerprints,
                "input_fingerprints",
            ),
        }
        with _cursor(self._connection) as cursor:
            cursor.execute(_INSERT_ANALYTICS_RUN_SQL, params)
            row = cursor.fetchone()
        if row is None:
            raise ForwardLabelPersistenceError("analytics run insert returned no id")
        return _row_int(row, "id", 0, "analytics_runs.id")

    def finish_label_generation_run(self, run_id: int, *, status: str) -> None:
        """Mark a label-generation run as succeeded or failed."""
        normalized_run_id = _positive_int(run_id, "run_id")
        if status not in {"succeeded", "failed"}:
            raise ForwardLabelPersistenceError(
                "status must be either succeeded or failed"
            )
        with _cursor(self._connection) as cursor:
            cursor.execute(
                _UPDATE_ANALYTICS_RUN_SQL,
                {
                    "run_id": normalized_run_id,
                    "status": status,
                    "finished_at": datetime.now(timezone.utc),
                },
            )

    def write_forward_labels(
        self,
        records: Sequence[ForwardLabelRecord],
    ) -> ForwardLabelWriteResult:
        """Upsert materialized labels without duplicating existing rows."""
        normalized_records = _validated_records(records)
        if not normalized_records:
            return ForwardLabelWriteResult(
                records_seen=0,
                rows_changed=0,
                label_version=None,
            )

        label_versions = {record.label_version for record in normalized_records}
        if len(label_versions) != 1:
            raise ForwardLabelPersistenceError(
                "all forward-label records in one write must use the same label_version"
            )

        rows_changed = 0
        seen_keys: set[tuple[int, date, int, int]] = set()
        ordered_records = sorted(
            normalized_records,
            key=lambda record: (
                record.security_id,
                record.label_date,
                record.horizon_days,
                record.label_version,
            ),
        )
        for record in ordered_records:
            key = (
                record.security_id,
                record.label_date,
                record.horizon_days,
                record.label_version,
            )
            if key in seen_keys:
                raise ForwardLabelPersistenceError(
                    "duplicate forward-label record for "
                    f"security_id={record.security_id}, "
                    f"label_date={record.label_date.isoformat()}, "
                    f"horizon_days={record.horizon_days}, "
                    f"label_version={record.label_version}"
                )
            seen_keys.add(key)

            with _cursor(self._connection) as cursor:
                cursor.execute(_UPSERT_FORWARD_LABEL_SQL, _record_params(record))
                if cursor.fetchone() is not None:
                    rows_changed += 1

        return ForwardLabelWriteResult(
            records_seen=len(ordered_records),
            rows_changed=rows_changed,
            label_version=ordered_records[0].label_version,
        )


def _validated_records(
    records: Sequence[ForwardLabelRecord],
) -> tuple[ForwardLabelRecord, ...]:
    if isinstance(records, (str, bytes)) or not isinstance(records, Sequence):
        raise ForwardLabelPersistenceError(
            "records must be a sequence of ForwardLabelRecord"
        )
    normalized = tuple(records)
    for index, record in enumerate(normalized, start=1):
        if not isinstance(record, ForwardLabelRecord):
            raise ForwardLabelPersistenceError(
                f"records[{index}] must be a ForwardLabelRecord"
            )
        _validate_record(record)
    return normalized


def _validate_record(record: ForwardLabelRecord) -> None:
    _positive_int(record.security_id, "security_id")
    _positive_int(record.horizon_days, "horizon_days")
    if record.horizon_date <= record.label_date:
        raise ForwardLabelPersistenceError("horizon_date must be after label_date")
    _positive_int(record.label_version, "label_version")
    _positive_decimal(record.start_adj_close, "start_adj_close")
    _positive_decimal(record.end_adj_close, "end_adj_close")
    _aware_datetime(record.horizon_close_at, "horizon_close_at")
    _aware_datetime(record.available_at, "available_at")
    if record.available_at < record.horizon_close_at:
        raise ForwardLabelPersistenceError(
            "available_at must be greater than or equal to horizon_close_at"
        )
    _positive_int(record.available_at_policy_id, "available_at_policy_id")
    _positive_int(record.computed_by_run_id, "computed_by_run_id")
    if record.benchmark_security_id is not None:
        _positive_int(record.benchmark_security_id, "benchmark_security_id")
    if (
        record.realized_excess_return is not None
        and record.benchmark_security_id is None
    ):
        raise ForwardLabelPersistenceError(
            "benchmark_security_id is required when realized_excess_return is set"
        )
    if not isinstance(record.metadata, Mapping):
        raise ForwardLabelPersistenceError("metadata must be a mapping")


def _record_params(record: ForwardLabelRecord) -> dict[str, Any]:
    return {
        "security_id": record.security_id,
        "label_date": record.label_date,
        "horizon_days": record.horizon_days,
        "horizon_date": record.horizon_date,
        "horizon_close_at": record.horizon_close_at,
        "label_version": record.label_version,
        "start_adj_close": record.start_adj_close,
        "end_adj_close": record.end_adj_close,
        "realized_raw_return": float(record.realized_raw_return),
        "benchmark_security_id": record.benchmark_security_id,
        "realized_excess_return": (
            None
            if record.realized_excess_return is None
            else float(record.realized_excess_return)
        ),
        "available_at": record.available_at,
        "available_at_policy_id": record.available_at_policy_id,
        "computed_by_run_id": record.computed_by_run_id,
        "metadata": _stable_json(record.metadata, "metadata"),
    }


def _price_observation(row: object) -> ForwardLabelPriceObservation:
    return ForwardLabelPriceObservation(
        security_id=_row_int(row, "security_id", 0, "prices_daily.security_id"),
        row=DailyPriceRow(
            ticker=_row_str(row, "ticker", 1, "securities.ticker"),
            date=_row_date(row, "date", 2, "prices_daily.date"),
            open=_row_decimal(row, "open", 3, "prices_daily.open"),
            high=_row_decimal(row, "high", 4, "prices_daily.high"),
            low=_row_decimal(row, "low", 5, "prices_daily.low"),
            close=_row_decimal(row, "close", 6, "prices_daily.close"),
            adj_close=_row_decimal(row, "adj_close", 7, "prices_daily.adj_close"),
            volume=_row_int(row, "volume", 8, "prices_daily.volume"),
            source=_row_str(row, "source_system", 9, "prices_daily.source_system"),
        ),
        available_at=_row_datetime(row, "available_at", 10, "prices_daily.available_at"),
        available_at_policy_id=_row_int(
            row,
            "available_at_policy_id",
            11,
            "prices_daily.available_at_policy_id",
        ),
    )


def _calendar_row(row: object) -> TradingCalendarRow:
    return TradingCalendarRow(
        date=_row_date(row, "date", 0, "trading_calendar.date"),
        is_session=_row_bool(row, "is_session", 1, "trading_calendar.is_session"),
        session_close=_row_optional_datetime(
            row,
            "session_close",
            2,
            "trading_calendar.session_close",
        ),
        is_early_close=_row_bool(
            row,
            "is_early_close",
            3,
            "trading_calendar.is_early_close",
        ),
    )


def _date_or_none(value: object, name: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ForwardLabelPersistenceError(f"{name} must be a date")
    return value


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ForwardLabelPersistenceError(f"{name} must be a positive integer")
    return value


def _non_empty_str(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ForwardLabelPersistenceError(f"{name} must be a non-empty string")
    return value.strip()


def _positive_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
        raise ForwardLabelPersistenceError(f"{name} must be a positive finite Decimal")
    return value


def _aware_datetime(value: object, name: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ForwardLabelPersistenceError(f"{name} must be timezone-aware")
    return value


def _stable_json(value: Mapping[str, Any], name: str) -> str:
    if not isinstance(value, Mapping):
        raise ForwardLabelPersistenceError(f"{name} must be a mapping")
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_default)
    except TypeError as exc:
        raise ForwardLabelPersistenceError(f"{name} must be JSON serializable") from exc


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise TypeError("naive datetime")
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def _row_value(row: object, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return row[index]  # type: ignore[index]


def _row_int(row: object, key: str, index: int, name: str) -> int:
    value = _row_value(row, key, index)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ForwardLabelPersistenceError(f"{name} returned by database must be an integer")
    return value


def _row_str(row: object, key: str, index: int, name: str) -> str:
    value = _row_value(row, key, index)
    if not isinstance(value, str) or not value.strip():
        raise ForwardLabelPersistenceError(
            f"{name} returned by database must be a non-empty string"
        )
    return value.strip()


def _row_date(row: object, key: str, index: int, name: str) -> date:
    value = _row_value(row, key, index)
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ForwardLabelPersistenceError(f"{name} returned by database must be a date")
    return value


def _row_datetime(row: object, key: str, index: int, name: str) -> datetime:
    return _aware_datetime(_row_value(row, key, index), f"{name} returned by database")


def _row_optional_datetime(
    row: object,
    key: str,
    index: int,
    name: str,
) -> datetime | None:
    value = _row_value(row, key, index)
    if value is None:
        return None
    return _aware_datetime(value, f"{name} returned by database")


def _row_decimal(row: object, key: str, index: int, name: str) -> Decimal:
    value = _row_value(row, key, index)
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    raise ForwardLabelPersistenceError(f"{name} returned by database must be numeric")


def _row_bool(row: object, key: str, index: int, name: str) -> bool:
    value = _row_value(row, key, index)
    if not isinstance(value, bool):
        raise ForwardLabelPersistenceError(f"{name} returned by database must be a boolean")
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


_SELECT_TRADING_CALENDAR_SQL = """
SELECT date, is_session, session_close, is_early_close
FROM silver.trading_calendar
ORDER BY date;
""".strip()

_SELECT_UNIVERSE_PRICES_SQL = """
WITH member_securities AS (
    SELECT DISTINCT security_id
    FROM silver.universe_membership
    WHERE universe_name = %(universe_name)s
      AND (%(label_end_date)s IS NULL OR valid_from <= %(label_end_date)s)
      AND (
          %(label_start_date)s IS NULL
          OR valid_to IS NULL
          OR valid_to >= %(label_start_date)s
      )
)
SELECT
    p.security_id,
    s.ticker,
    p.date,
    p.open,
    p.high,
    p.low,
    p.close,
    p.adj_close,
    p.volume,
    p.source_system,
    p.available_at,
    p.available_at_policy_id
FROM silver.prices_daily AS p
JOIN silver.securities AS s
  ON s.id = p.security_id
JOIN member_securities AS m
  ON m.security_id = p.security_id
WHERE (%(price_start_date)s IS NULL OR p.date >= %(price_start_date)s)
  AND (%(price_end_date)s IS NULL OR p.date <= %(price_end_date)s)
ORDER BY s.ticker, p.date;
""".strip()

_SELECT_LABEL_DATES_SQL = """
SELECT DISTINCT p.security_id, p.date
FROM silver.prices_daily AS p
JOIN silver.universe_membership AS um
  ON um.security_id = p.security_id
 AND um.universe_name = %(universe_name)s
 AND um.valid_from <= p.date
 AND (um.valid_to IS NULL OR um.valid_to >= p.date)
WHERE (%(label_start_date)s IS NULL OR p.date >= %(label_start_date)s)
  AND (%(label_end_date)s IS NULL OR p.date <= %(label_end_date)s)
ORDER BY p.security_id, p.date;
""".strip()

_SELECT_POLICY_VERSIONS_SQL = """
SELECT id, name, version
FROM silver.available_at_policies
WHERE id = ANY(%(available_at_policy_ids)s)
ORDER BY name, version;
""".strip()

_INSERT_ANALYTICS_RUN_SQL = """
INSERT INTO silver.analytics_runs (
    run_kind,
    code_git_sha,
    available_at_policy_versions,
    parameters,
    input_fingerprints,
    status
) VALUES (
    'label_generation',
    %(code_git_sha)s,
    %(available_at_policy_versions)s::jsonb,
    %(parameters)s::jsonb,
    %(input_fingerprints)s::jsonb,
    'running'
)
RETURNING id;
""".strip()

_UPDATE_ANALYTICS_RUN_SQL = """
UPDATE silver.analytics_runs
SET status = %(status)s,
    finished_at = %(finished_at)s
WHERE id = %(run_id)s;
""".strip()

_UPSERT_FORWARD_LABEL_SQL = """
INSERT INTO silver.forward_return_labels (
    security_id,
    label_date,
    horizon_days,
    horizon_date,
    horizon_close_at,
    label_version,
    start_adj_close,
    end_adj_close,
    realized_raw_return,
    benchmark_security_id,
    realized_excess_return,
    available_at,
    available_at_policy_id,
    computed_by_run_id,
    metadata
) VALUES (
    %(security_id)s,
    %(label_date)s,
    %(horizon_days)s,
    %(horizon_date)s,
    %(horizon_close_at)s,
    %(label_version)s,
    %(start_adj_close)s,
    %(end_adj_close)s,
    %(realized_raw_return)s,
    %(benchmark_security_id)s,
    %(realized_excess_return)s,
    %(available_at)s,
    %(available_at_policy_id)s,
    %(computed_by_run_id)s,
    %(metadata)s::jsonb
)
ON CONFLICT (security_id, label_date, horizon_days, label_version) DO UPDATE SET
    horizon_date = EXCLUDED.horizon_date,
    horizon_close_at = EXCLUDED.horizon_close_at,
    start_adj_close = EXCLUDED.start_adj_close,
    end_adj_close = EXCLUDED.end_adj_close,
    realized_raw_return = EXCLUDED.realized_raw_return,
    benchmark_security_id = EXCLUDED.benchmark_security_id,
    realized_excess_return = EXCLUDED.realized_excess_return,
    available_at = EXCLUDED.available_at,
    available_at_policy_id = EXCLUDED.available_at_policy_id,
    computed_by_run_id = EXCLUDED.computed_by_run_id,
    computed_at = now(),
    metadata = EXCLUDED.metadata
WHERE
    silver.forward_return_labels.horizon_date IS DISTINCT FROM EXCLUDED.horizon_date
    OR silver.forward_return_labels.horizon_close_at IS DISTINCT FROM
        EXCLUDED.horizon_close_at
    OR silver.forward_return_labels.start_adj_close IS DISTINCT FROM
        EXCLUDED.start_adj_close
    OR silver.forward_return_labels.end_adj_close IS DISTINCT FROM
        EXCLUDED.end_adj_close
    OR silver.forward_return_labels.realized_raw_return IS DISTINCT FROM
        EXCLUDED.realized_raw_return
    OR silver.forward_return_labels.benchmark_security_id IS DISTINCT FROM
        EXCLUDED.benchmark_security_id
    OR silver.forward_return_labels.realized_excess_return IS DISTINCT FROM
        EXCLUDED.realized_excess_return
    OR silver.forward_return_labels.available_at IS DISTINCT FROM
        EXCLUDED.available_at
    OR silver.forward_return_labels.available_at_policy_id IS DISTINCT FROM
        EXCLUDED.available_at_policy_id
    OR silver.forward_return_labels.metadata IS DISTINCT FROM EXCLUDED.metadata
RETURNING id;
""".strip()
