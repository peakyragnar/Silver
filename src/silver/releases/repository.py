"""Persistence helpers for earnings release events."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


SEC_8K_MATERIAL_POLICY_NAME = "sec_8k_material"
DEFAULT_POLICY_VERSION = 1
MARKET_TIMEZONE = ZoneInfo("America/New_York")
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)


class EarningsReleaseRepositoryError(ValueError):
    """Raised when earnings release events cannot be persisted safely."""


@dataclass(frozen=True, slots=True)
class EarningsReleasePolicy:
    """Available-at policy metadata for release events."""

    id: int
    name: str
    version: int
    rule: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class EarningsReleaseEventRecord:
    """One normalized earnings release event ready for persistence."""

    security_id: int
    accession_number: str
    form_type: str
    item_codes: str
    filing_date: date
    report_date: date | None
    accepted_at: datetime
    release_timing: str
    release_available_at: datetime
    available_at_policy_id: int
    fiscal_year: int
    fiscal_period: str
    period_end_date: date
    primary_document: str
    exhibit_document: str | None
    submissions_raw_object_id: int
    archive_index_raw_object_id: int | None
    exhibit_raw_object_id: int | None
    normalized_by_run_id: int
    matched_confidence: str
    match_method: str
    metadata: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class EarningsReleaseWriteResult:
    """Summary of an earnings-release persistence call."""

    rows_seen: int
    rows_written: int


class EarningsReleaseEventRepository:
    """Persist normalized earnings release events."""

    def __init__(self, connection: Any):
        self._connection = connection

    def load_policy(
        self,
        *,
        version: int = DEFAULT_POLICY_VERSION,
    ) -> EarningsReleasePolicy:
        """Load the SEC 8-K material available-at policy."""
        normalized_version = _positive_int(version, "version")
        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_POLICY_SQL,
                {
                    "name": SEC_8K_MATERIAL_POLICY_NAME,
                    "version": normalized_version,
                },
            )
            row = cursor.fetchone()
        if row is None:
            raise EarningsReleaseRepositoryError(
                f"{SEC_8K_MATERIAL_POLICY_NAME} policy version "
                f"{normalized_version} was not found"
            )
        return _policy_record(row)

    def load_trading_calendar(self) -> TradingCalendar:
        """Load seeded trading calendar rows for release timing classification."""
        with _cursor(self._connection) as cursor:
            cursor.execute(_SELECT_TRADING_CALENDAR_SQL, {})
            rows = cursor.fetchall()
        return TradingCalendar(tuple(_calendar_row(row) for row in rows))

    def write_events(
        self,
        records: Sequence[EarningsReleaseEventRecord],
    ) -> EarningsReleaseWriteResult:
        """Upsert release events."""
        normalized_records = _validated_records(records)
        rows_written = 0
        for record in normalized_records:
            with _cursor(self._connection) as cursor:
                cursor.execute(_UPSERT_EVENT_SQL, _record_params(record))
            rows_written += 1
        return EarningsReleaseWriteResult(
            rows_seen=len(normalized_records),
            rows_written=rows_written,
        )

    def count_linked_income_fundamentals(
        self,
        records: Sequence[EarningsReleaseEventRecord],
    ) -> int:
        """Count income-statement fundamentals matched by period identity."""
        normalized_records = _validated_records(records)
        if not normalized_records:
            return 0
        total = 0
        for record in normalized_records:
            with _cursor(self._connection) as cursor:
                cursor.execute(
                    _COUNT_LINKED_INCOME_FUNDAMENTALS_SQL,
                    {
                        "security_id": record.security_id,
                        "fiscal_year": record.fiscal_year,
                        "fiscal_period": record.fiscal_period,
                        "period_end_date": record.period_end_date,
                    },
                )
                row = cursor.fetchone()
            total += _row_int(row, "count", 0, "linked income fundamental count")
        return total


def release_available_at(
    accepted_at: datetime,
    *,
    policy: EarningsReleasePolicy,
) -> datetime:
    """Compute release-event availability from the SEC 8-K material policy."""
    if accepted_at.tzinfo is None or accepted_at.utcoffset() is None:
        raise EarningsReleaseRepositoryError("accepted_at must be timezone-aware")
    rule = policy.rule
    if rule.get("type") != "timestamp_plus_duration":
        raise EarningsReleaseRepositoryError(
            f"{policy.name} policy type must be timestamp_plus_duration"
        )
    if rule.get("base") != "accepted_at":
        raise EarningsReleaseRepositoryError(
            f"{policy.name} policy base must be accepted_at"
        )
    return accepted_at.astimezone(timezone.utc) + _duration(rule.get("duration"))


def release_market_timing(
    accepted_at: datetime,
    *,
    calendar: TradingCalendar,
) -> str:
    """Classify an accepted timestamp as BMO, RTH, AMC, or non-session."""
    if accepted_at.tzinfo is None or accepted_at.utcoffset() is None:
        raise EarningsReleaseRepositoryError("accepted_at must be timezone-aware")
    local = accepted_at.astimezone(MARKET_TIMEZONE)
    row = calendar.row_for(local.date())
    if not row.is_session:
        return "non_trading_day"
    local_time = local.time()
    if local_time < MARKET_OPEN:
        return "bmo"
    if local_time >= MARKET_CLOSE:
        return "amc"
    return "rth"


def _validated_records(
    records: Sequence[EarningsReleaseEventRecord],
) -> tuple[EarningsReleaseEventRecord, ...]:
    if isinstance(records, (str, bytes)) or not isinstance(records, Sequence):
        raise EarningsReleaseRepositoryError("records must be a sequence")
    return tuple(_validated_record(record) for record in records)


def _validated_record(record: EarningsReleaseEventRecord) -> EarningsReleaseEventRecord:
    if not isinstance(record, EarningsReleaseEventRecord):
        raise EarningsReleaseRepositoryError(
            "records must contain EarningsReleaseEventRecord items"
        )
    _positive_int(record.security_id, "security_id")
    if re.fullmatch(r"\d{10}-\d{2}-\d{6}", record.accession_number) is None:
        raise EarningsReleaseRepositoryError("accession_number has invalid format")
    if record.form_type not in {"8-K", "8-K/A"}:
        raise EarningsReleaseRepositoryError("form_type must be 8-K or 8-K/A")
    if "2.02" not in record.item_codes:
        raise EarningsReleaseRepositoryError("item_codes must include 2.02")
    _validate_date(record.filing_date, "filing_date")
    if record.report_date is not None:
        _validate_date(record.report_date, "report_date")
    _validate_datetime(record.accepted_at, "accepted_at")
    if record.release_timing not in {"bmo", "rth", "amc", "non_trading_day"}:
        raise EarningsReleaseRepositoryError(
            "release_timing must be bmo, rth, amc, or non_trading_day"
        )
    _validate_datetime(record.release_available_at, "release_available_at")
    if record.release_available_at <= record.accepted_at:
        raise EarningsReleaseRepositoryError(
            "release_available_at must be after accepted_at"
        )
    _positive_int(record.available_at_policy_id, "available_at_policy_id")
    if record.fiscal_year < 1900 or record.fiscal_year > 2100:
        raise EarningsReleaseRepositoryError("fiscal_year must be between 1900 and 2100")
    if record.fiscal_period not in {"FY", "Q1", "Q2", "Q3", "Q4"}:
        raise EarningsReleaseRepositoryError("fiscal_period must be FY or Q1-Q4")
    _validate_date(record.period_end_date, "period_end_date")
    _required_label(record.primary_document, "primary_document")
    if record.exhibit_document is not None:
        _required_label(record.exhibit_document, "exhibit_document")
    _positive_int(record.submissions_raw_object_id, "submissions_raw_object_id")
    if record.archive_index_raw_object_id is not None:
        _positive_int(record.archive_index_raw_object_id, "archive_index_raw_object_id")
    if record.exhibit_raw_object_id is not None:
        _positive_int(record.exhibit_raw_object_id, "exhibit_raw_object_id")
    _positive_int(record.normalized_by_run_id, "normalized_by_run_id")
    if record.matched_confidence not in {"high", "medium", "low"}:
        raise EarningsReleaseRepositoryError(
            "matched_confidence must be high, medium, or low"
        )
    _required_label(record.match_method, "match_method")
    if not isinstance(record.metadata, Mapping):
        raise EarningsReleaseRepositoryError("metadata must be a mapping")
    return record


def _record_params(record: EarningsReleaseEventRecord) -> dict[str, Any]:
    return {
        "security_id": record.security_id,
        "accession_number": record.accession_number,
        "form_type": record.form_type,
        "item_codes": record.item_codes,
        "filing_date": record.filing_date,
        "report_date": record.report_date,
        "accepted_at": record.accepted_at,
        "release_timing": record.release_timing,
        "release_available_at": record.release_available_at,
        "available_at_policy_id": record.available_at_policy_id,
        "fiscal_year": record.fiscal_year,
        "fiscal_period": record.fiscal_period,
        "period_end_date": record.period_end_date,
        "primary_document": record.primary_document,
        "exhibit_document": record.exhibit_document,
        "submissions_raw_object_id": record.submissions_raw_object_id,
        "archive_index_raw_object_id": record.archive_index_raw_object_id,
        "exhibit_raw_object_id": record.exhibit_raw_object_id,
        "normalized_by_run_id": record.normalized_by_run_id,
        "matched_confidence": record.matched_confidence,
        "match_method": record.match_method,
        "metadata": _stable_json(record.metadata),
    }


def _policy_record(row: object) -> EarningsReleasePolicy:
    rule = _row_value(row, "rule", 3)
    if isinstance(rule, str):
        try:
            rule = json.loads(rule)
        except json.JSONDecodeError as exc:
            raise EarningsReleaseRepositoryError(
                "available_at policy rule must be JSON"
            ) from exc
    if not isinstance(rule, Mapping):
        raise EarningsReleaseRepositoryError(
            "available_at policy rule must be a mapping"
        )
    return EarningsReleasePolicy(
        id=_row_int(row, "id", 0, "available_at_policies.id"),
        name=_row_str(row, "name", 1, "available_at_policies.name"),
        version=_row_int(row, "version", 2, "available_at_policies.version"),
        rule=rule,
    )


def _calendar_row(row: object) -> TradingCalendarRow:
    return TradingCalendarRow(
        date=_row_date(row, "date", 0, "trading_calendar.date"),
        is_session=_row_bool(row, "is_session", 1),
        session_close=_row_optional_datetime(
            row,
            "session_close",
            2,
            "trading_calendar.session_close",
        ),
        is_early_close=_row_bool(row, "is_early_close", 3),
    )


def _duration(value: object) -> timedelta:
    if not isinstance(value, str):
        raise EarningsReleaseRepositoryError("duration must be an ISO-8601 duration")
    match = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", value)
    if match is None:
        raise EarningsReleaseRepositoryError(
            "duration must be a simple ISO-8601 time duration"
        )
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    delta = timedelta(hours=hours, minutes=minutes, seconds=seconds)
    if delta <= timedelta(0):
        raise EarningsReleaseRepositoryError("duration must be positive")
    return delta


def _stable_json(value: Mapping[str, Any]) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    except TypeError as exc:
        raise EarningsReleaseRepositoryError(
            "metadata must be JSON serializable"
        ) from exc


def _required_label(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EarningsReleaseRepositoryError(f"{name} must be a non-empty string")
    return value.strip()


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise EarningsReleaseRepositoryError(f"{name} must be a positive integer")
    return value


def _validate_date(value: object, name: str) -> None:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise EarningsReleaseRepositoryError(f"{name} must be a date")


def _validate_datetime(value: object, name: str) -> None:
    if not isinstance(value, datetime):
        raise EarningsReleaseRepositoryError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise EarningsReleaseRepositoryError(f"{name} must be timezone-aware")


def _row_value(row: object, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return row[index]  # type: ignore[index]


def _row_int(row: object | None, key: str, index: int, name: str) -> int:
    if row is None:
        raise EarningsReleaseRepositoryError(f"{name} was not returned")
    value = _row_value(row, key, index)
    if isinstance(value, bool) or not isinstance(value, int):
        raise EarningsReleaseRepositoryError(f"{name} must be an integer")
    return value


def _row_str(row: object, key: str, index: int, name: str) -> str:
    value = _row_value(row, key, index)
    if not isinstance(value, str) or not value.strip():
        raise EarningsReleaseRepositoryError(f"{name} must be a non-empty string")
    return value.strip()


def _row_bool(row: object, key: str, index: int) -> bool:
    value = _row_value(row, key, index)
    if not isinstance(value, bool):
        raise EarningsReleaseRepositoryError(f"{key} must be a boolean")
    return value


def _row_date(row: object, key: str, index: int, name: str) -> date:
    value = _row_value(row, key, index)
    if isinstance(value, datetime) or not isinstance(value, date):
        raise EarningsReleaseRepositoryError(f"{name} must be a date")
    return value


def _row_optional_datetime(
    row: object,
    key: str,
    index: int,
    name: str,
) -> datetime | None:
    value = _row_value(row, key, index)
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise EarningsReleaseRepositoryError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise EarningsReleaseRepositoryError(f"{name} must be timezone-aware")
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
WHERE name = %(name)s
  AND version = %(version)s
LIMIT 1;
""".strip()

_SELECT_TRADING_CALENDAR_SQL = """
SELECT date, is_session, session_close, is_early_close
FROM silver.trading_calendar
ORDER BY date;
""".strip()

_UPSERT_EVENT_SQL = """
INSERT INTO silver.earnings_release_events (
    security_id,
    accession_number,
    form_type,
    item_codes,
    filing_date,
    report_date,
    accepted_at,
    release_timing,
    release_available_at,
    available_at_policy_id,
    fiscal_year,
    fiscal_period,
    period_end_date,
    primary_document,
    exhibit_document,
    submissions_raw_object_id,
    archive_index_raw_object_id,
    exhibit_raw_object_id,
    normalized_by_run_id,
    matched_confidence,
    match_method,
    metadata
) VALUES (
    %(security_id)s,
    %(accession_number)s,
    %(form_type)s,
    %(item_codes)s,
    %(filing_date)s,
    %(report_date)s,
    %(accepted_at)s,
    %(release_timing)s,
    %(release_available_at)s,
    %(available_at_policy_id)s,
    %(fiscal_year)s,
    %(fiscal_period)s,
    %(period_end_date)s,
    %(primary_document)s,
    %(exhibit_document)s,
    %(submissions_raw_object_id)s,
    %(archive_index_raw_object_id)s,
    %(exhibit_raw_object_id)s,
    %(normalized_by_run_id)s,
    %(matched_confidence)s,
    %(match_method)s,
    %(metadata)s::jsonb
)
ON CONFLICT (security_id, accession_number) DO UPDATE SET
    form_type = EXCLUDED.form_type,
    item_codes = EXCLUDED.item_codes,
    filing_date = EXCLUDED.filing_date,
    report_date = EXCLUDED.report_date,
    accepted_at = EXCLUDED.accepted_at,
    release_timing = EXCLUDED.release_timing,
    release_available_at = EXCLUDED.release_available_at,
    available_at_policy_id = EXCLUDED.available_at_policy_id,
    fiscal_year = EXCLUDED.fiscal_year,
    fiscal_period = EXCLUDED.fiscal_period,
    period_end_date = EXCLUDED.period_end_date,
    primary_document = EXCLUDED.primary_document,
    exhibit_document = EXCLUDED.exhibit_document,
    submissions_raw_object_id = EXCLUDED.submissions_raw_object_id,
    archive_index_raw_object_id = EXCLUDED.archive_index_raw_object_id,
    exhibit_raw_object_id = EXCLUDED.exhibit_raw_object_id,
    normalized_by_run_id = EXCLUDED.normalized_by_run_id,
    matched_confidence = EXCLUDED.matched_confidence,
    match_method = EXCLUDED.match_method,
    metadata = EXCLUDED.metadata
WHERE
    silver.earnings_release_events.form_type IS DISTINCT FROM EXCLUDED.form_type
    OR silver.earnings_release_events.item_codes IS DISTINCT FROM EXCLUDED.item_codes
    OR silver.earnings_release_events.filing_date IS DISTINCT FROM EXCLUDED.filing_date
    OR silver.earnings_release_events.report_date IS DISTINCT FROM EXCLUDED.report_date
    OR silver.earnings_release_events.accepted_at IS DISTINCT FROM EXCLUDED.accepted_at
    OR silver.earnings_release_events.release_timing IS DISTINCT FROM
        EXCLUDED.release_timing
    OR silver.earnings_release_events.release_available_at IS DISTINCT FROM
        EXCLUDED.release_available_at
    OR silver.earnings_release_events.available_at_policy_id IS DISTINCT FROM
        EXCLUDED.available_at_policy_id
    OR silver.earnings_release_events.fiscal_year IS DISTINCT FROM EXCLUDED.fiscal_year
    OR silver.earnings_release_events.fiscal_period IS DISTINCT FROM
        EXCLUDED.fiscal_period
    OR silver.earnings_release_events.period_end_date IS DISTINCT FROM
        EXCLUDED.period_end_date
    OR silver.earnings_release_events.primary_document IS DISTINCT FROM
        EXCLUDED.primary_document
    OR silver.earnings_release_events.exhibit_document IS DISTINCT FROM
        EXCLUDED.exhibit_document
    OR silver.earnings_release_events.submissions_raw_object_id IS DISTINCT FROM
        EXCLUDED.submissions_raw_object_id
    OR silver.earnings_release_events.archive_index_raw_object_id IS DISTINCT FROM
        EXCLUDED.archive_index_raw_object_id
    OR silver.earnings_release_events.exhibit_raw_object_id IS DISTINCT FROM
        EXCLUDED.exhibit_raw_object_id
    OR silver.earnings_release_events.normalized_by_run_id IS DISTINCT FROM
        EXCLUDED.normalized_by_run_id
    OR silver.earnings_release_events.matched_confidence IS DISTINCT FROM
        EXCLUDED.matched_confidence
    OR silver.earnings_release_events.match_method IS DISTINCT FROM EXCLUDED.match_method
    OR silver.earnings_release_events.metadata IS DISTINCT FROM EXCLUDED.metadata;
""".strip()

_COUNT_LINKED_INCOME_FUNDAMENTALS_SQL = """
SELECT count(*)::int
FROM silver.fundamental_values
WHERE security_id = %(security_id)s
  AND fiscal_year = %(fiscal_year)s
  AND fiscal_period = %(fiscal_period)s
  AND period_end_date = %(period_end_date)s
  AND statement_type = 'income_statement';
""".strip()
