"""Feature-store persistence helpers."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from silver.features.momentum_12_1 import (
    AdjustedDailyPriceObservation,
    NumericFeatureDefinition,
)
from silver.time.trading_calendar import TradingCalendarRow


class FeatureStoreError(ValueError):
    """Raised when feature-store persistence would violate Silver rules."""


@dataclass(frozen=True, slots=True)
class FeatureDefinitionRecord:
    id: int
    name: str
    version: int
    kind: str
    computation_spec: Mapping[str, Any]
    definition_hash: str
    notes: str | None


@dataclass(frozen=True, slots=True)
class AvailableAtPolicyRecord:
    id: int
    name: str
    version: int
    rule: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class UniverseMembershipRecord:
    security_id: int
    ticker: str
    valid_from: date
    valid_to: date | None

    def is_active_on(self, asof_date: date) -> bool:
        return self.valid_from <= asof_date and (
            self.valid_to is None or self.valid_to >= asof_date
        )


@dataclass(frozen=True, slots=True)
class FeatureValueWrite:
    security_id: int
    asof_date: date
    feature_definition_id: int
    value: float
    available_at: datetime
    available_at_policy_id: int
    computed_by_run_id: int
    source_metadata: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class FeatureValueWriteResult:
    rows_written: int


class FeatureStoreRepository:
    """Read and write Silver feature-store rows.

    The repository owns SQL shape and validation but not transaction boundaries.
    Callers provide a DB-API compatible connection and decide when to commit.
    """

    def __init__(self, connection: Any):
        self._connection = connection

    def ensure_feature_definition(
        self,
        definition: NumericFeatureDefinition,
        *,
        notes: str | None = None,
    ) -> FeatureDefinitionRecord:
        """Insert a deterministic definition, or validate the existing one."""
        payload = feature_definition_payload(definition)
        params = {
            "name": definition.name,
            "version": definition.version,
            "kind": definition.kind,
            "computation_spec": _json_dumps(payload["computation_spec"]),
            "definition_hash": feature_definition_hash(definition),
            "notes": notes,
        }
        with _cursor(self._connection) as cursor:
            cursor.execute(_INSERT_FEATURE_DEFINITION_SQL, params)
            row = cursor.fetchone()
        if row is not None:
            return _feature_definition_record(row)

        existing = self._load_feature_definition(
            name=definition.name,
            version=definition.version,
        )
        if existing.definition_hash != params["definition_hash"]:
            raise FeatureStoreError(
                f"feature definition {definition.name} v{definition.version} "
                "already exists with a different definition_hash"
            )
        if existing.kind != definition.kind:
            raise FeatureStoreError(
                f"feature definition {definition.name} v{definition.version} "
                f"has kind {existing.kind}; expected {definition.kind}"
            )
        return existing

    def load_available_at_policy(
        self,
        *,
        name: str,
        version: int,
    ) -> AvailableAtPolicyRecord:
        normalized_name = _non_empty_str(name, "available_at policy name")
        normalized_version = _positive_int(version, "available_at policy version")
        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_POLICY_BY_NAME_VERSION_SQL,
                {"name": normalized_name, "version": normalized_version},
            )
            row = cursor.fetchone()
        if row is None:
            raise FeatureStoreError(
                f"available_at policy {normalized_name} v{normalized_version} "
                "was not found"
            )
        return _available_at_policy_record(row)

    def load_universe_memberships(
        self,
        *,
        universe_name: str,
        start_date: date | None,
        end_date: date | None,
    ) -> tuple[UniverseMembershipRecord, ...]:
        normalized_universe = _non_empty_str(universe_name, "universe_name")
        _validate_optional_date(start_date, "start_date")
        _validate_optional_date(end_date, "end_date")
        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_UNIVERSE_MEMBERSHIPS_SQL,
                {
                    "universe_name": normalized_universe,
                    "start_date": start_date,
                    "end_date": end_date,
                },
            )
            rows = cursor.fetchall()
        return tuple(_universe_membership_record(row) for row in rows)

    def load_trading_calendar(
        self,
        *,
        end_date: date | None,
    ) -> tuple[TradingCalendarRow, ...]:
        _validate_optional_date(end_date, "end_date")
        with _cursor(self._connection) as cursor:
            cursor.execute(_SELECT_TRADING_CALENDAR_SQL, {"end_date": end_date})
            rows = cursor.fetchall()
        return tuple(_trading_calendar_row(row) for row in rows)

    def load_adjusted_prices(
        self,
        *,
        security_ids: Sequence[int],
        end_date: date | None,
        available_at_policy_id: int,
    ) -> tuple[tuple[int, AdjustedDailyPriceObservation], ...]:
        normalized_security_ids = tuple(
            sorted(
                {
                    _positive_int(security_id, "security_id")
                    for security_id in security_ids
                }
            )
        )
        _validate_optional_date(end_date, "end_date")
        normalized_policy_id = _positive_int(
            available_at_policy_id,
            "available_at_policy_id",
        )
        if not normalized_security_ids:
            return ()

        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_ADJUSTED_PRICES_SQL,
                {
                    "security_ids": list(normalized_security_ids),
                    "end_date": end_date,
                    "available_at_policy_id": normalized_policy_id,
                },
            )
            rows = cursor.fetchall()
        return tuple(_adjusted_price_row(row) for row in rows)

    def write_feature_values(
        self,
        values: Sequence[FeatureValueWrite],
    ) -> FeatureValueWriteResult:
        if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
            raise FeatureStoreError("values must be a sequence of FeatureValueWrite")

        normalized_values = tuple(values)
        seen_keys: set[tuple[int, date, int]] = set()
        for value in normalized_values:
            _validate_feature_value_write(value)
            key = (value.security_id, value.asof_date, value.feature_definition_id)
            if key in seen_keys:
                raise FeatureStoreError(
                    "duplicate feature value write for "
                    f"security_id {value.security_id}, "
                    f"asof_date {value.asof_date.isoformat()}, "
                    f"feature_definition_id {value.feature_definition_id}"
                )
            seen_keys.add(key)

        for value in sorted(
            normalized_values,
            key=lambda item: (
                item.security_id,
                item.asof_date,
                item.feature_definition_id,
            ),
        ):
            params = {
                "security_id": value.security_id,
                "asof_date": value.asof_date,
                "feature_definition_id": value.feature_definition_id,
                "value": value.value,
                "available_at": value.available_at,
                "available_at_policy_id": value.available_at_policy_id,
                "computed_by_run_id": value.computed_by_run_id,
                "source_metadata": _json_dumps(value.source_metadata),
            }
            with _cursor(self._connection) as cursor:
                cursor.execute(_UPSERT_FEATURE_VALUE_SQL, params)

        return FeatureValueWriteResult(rows_written=len(normalized_values))

    def create_feature_generation_run(
        self,
        *,
        code_git_sha: str,
        feature_set_hash: str,
        available_at_policy_versions: Mapping[str, Any],
        parameters: Mapping[str, Any],
        input_fingerprints: Mapping[str, Any] | None = None,
    ) -> int:
        normalized_sha = _non_empty_str(code_git_sha, "code_git_sha")
        normalized_feature_set_hash = _non_empty_str(
            feature_set_hash,
            "feature_set_hash",
        )
        with _cursor(self._connection) as cursor:
            cursor.execute(
                _INSERT_ANALYTICS_RUN_SQL,
                {
                    "code_git_sha": normalized_sha,
                    "feature_set_hash": normalized_feature_set_hash,
                    "available_at_policy_versions": _json_dumps(
                        available_at_policy_versions
                    ),
                    "parameters": _json_dumps(parameters),
                    "input_fingerprints": _json_dumps(input_fingerprints or {}),
                },
            )
            row = cursor.fetchone()
        if row is None:
            raise FeatureStoreError("analytics run insert did not return an id")
        return _row_int(row, "id", 0, "analytics_runs.id")

    def finish_analytics_run(self, *, run_id: int, status: str) -> None:
        normalized_run_id = _positive_int(run_id, "run_id")
        normalized_status = _non_empty_str(status, "status")
        if normalized_status not in {"succeeded", "failed"}:
            raise FeatureStoreError("status must be succeeded or failed")
        with _cursor(self._connection) as cursor:
            cursor.execute(
                _FINISH_ANALYTICS_RUN_SQL,
                {"run_id": normalized_run_id, "status": normalized_status},
            )

    def _load_feature_definition(
        self,
        *,
        name: str,
        version: int,
    ) -> FeatureDefinitionRecord:
        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_FEATURE_DEFINITION_SQL,
                {"name": name, "version": version},
            )
            row = cursor.fetchone()
        if row is None:
            raise FeatureStoreError(
                f"feature definition {name} v{version} was not found after conflict"
            )
        return _feature_definition_record(row)


def feature_definition_payload(
    definition: NumericFeatureDefinition,
) -> dict[str, Any]:
    """Return the canonical JSON payload hashed for feature definitions."""
    if not isinstance(definition, NumericFeatureDefinition):
        raise FeatureStoreError("definition must be a NumericFeatureDefinition")
    _non_empty_str(definition.name, "feature definition name")
    _positive_int(definition.version, "feature definition version")
    if definition.kind != "numeric":
        raise FeatureStoreError("feature definition kind must be numeric")
    computation_spec = _json_object(definition.computation_spec, "computation_spec")
    return {
        "name": definition.name,
        "version": definition.version,
        "kind": definition.kind,
        "computation_spec": computation_spec,
    }


def feature_definition_hash(definition: NumericFeatureDefinition) -> str:
    """Return a stable SHA-256 over feature-definition identity and logic."""
    payload = feature_definition_payload(definition)
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def _validate_feature_value_write(value: object) -> None:
    if not isinstance(value, FeatureValueWrite):
        raise FeatureStoreError("values must be FeatureValueWrite instances")
    _positive_int(value.security_id, "security_id")
    _validate_date(value.asof_date, "asof_date")
    _positive_int(value.feature_definition_id, "feature_definition_id")
    if isinstance(value.value, bool) or not isinstance(value.value, (float, int)):
        raise FeatureStoreError("feature value must be a finite number")
    if not math.isfinite(float(value.value)):
        raise FeatureStoreError("feature value must be finite")
    _require_aware(value.available_at, "available_at")
    _positive_int(value.available_at_policy_id, "available_at_policy_id")
    _positive_int(value.computed_by_run_id, "computed_by_run_id")
    _json_object(value.source_metadata, "source_metadata")


def _feature_definition_record(row: object) -> FeatureDefinitionRecord:
    computation_spec = _json_object(
        _json_loads_if_needed(_row_value(row, "computation_spec", 4)),
        "feature_definitions.computation_spec",
    )
    return FeatureDefinitionRecord(
        id=_row_int(row, "id", 0, "feature_definitions.id"),
        name=_row_str(row, "name", 1, "feature_definitions.name"),
        version=_row_int(row, "version", 2, "feature_definitions.version"),
        kind=_row_str(row, "kind", 3, "feature_definitions.kind"),
        computation_spec=computation_spec,
        definition_hash=_row_str(
            row,
            "definition_hash",
            5,
            "feature_definitions.definition_hash",
        ),
        notes=_optional_row_str(row, "notes", 6, "feature_definitions.notes"),
    )


def _available_at_policy_record(row: object) -> AvailableAtPolicyRecord:
    rule = _json_object(
        _json_loads_if_needed(_row_value(row, "rule", 3)),
        "available_at_policies.rule",
    )
    return AvailableAtPolicyRecord(
        id=_row_int(row, "id", 0, "available_at_policies.id"),
        name=_row_str(row, "name", 1, "available_at_policies.name"),
        version=_row_int(row, "version", 2, "available_at_policies.version"),
        rule=rule,
    )


def _universe_membership_record(row: object) -> UniverseMembershipRecord:
    return UniverseMembershipRecord(
        security_id=_row_int(row, "security_id", 0, "universe_membership.security_id"),
        ticker=_row_str(row, "ticker", 1, "securities.ticker"),
        valid_from=_row_date(row, "valid_from", 2, "universe_membership.valid_from"),
        valid_to=_optional_row_date(
            row,
            "valid_to",
            3,
            "universe_membership.valid_to",
        ),
    )


def _trading_calendar_row(row: object) -> TradingCalendarRow:
    return TradingCalendarRow(
        date=_row_date(row, "date", 0, "trading_calendar.date"),
        is_session=_row_bool(row, "is_session", 1, "trading_calendar.is_session"),
        session_close=_optional_row_datetime(
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


def _adjusted_price_row(
    row: object,
) -> tuple[int, AdjustedDailyPriceObservation]:
    return (
        _row_int(row, "security_id", 0, "prices_daily.security_id"),
        AdjustedDailyPriceObservation(
            price_date=_row_date(row, "date", 1, "prices_daily.date"),
            adjusted_close=_row_decimal(row, "adj_close", 2, "prices_daily.adj_close"),
            available_at=_row_datetime(
                row,
                "available_at",
                3,
                "prices_daily.available_at",
            ),
        ),
    )


def _json_dumps(value: Mapping[str, Any]) -> str:
    _json_object(value, "json value")
    return json.dumps(
        _json_normalize(value),
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_loads_if_needed(value: object) -> object:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise FeatureStoreError("database returned invalid JSON") from exc
    return value


def _json_object(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise FeatureStoreError(f"{name} must be a JSON object")
    normalized = _json_normalize(value)
    if not isinstance(normalized, dict):
        raise FeatureStoreError(f"{name} must be a JSON object")
    return normalized


def _json_normalize(value: object) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_normalize(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_normalize(item) for item in value]
    if isinstance(value, list):
        return [_json_normalize(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise FeatureStoreError(f"value is not JSON serializable: {value!r}")


def _validate_optional_date(value: date | None, name: str) -> None:
    if value is not None:
        _validate_date(value, name)


def _validate_date(value: object, name: str) -> None:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise FeatureStoreError(f"{name} must be a date")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FeatureStoreError(f"{field_name} must be timezone-aware")


def _non_empty_str(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FeatureStoreError(f"{name} must be a non-empty string")
    return value.strip()


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FeatureStoreError(f"{name} must be a positive integer")
    return value


def _row_value(row: object, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return row[index]  # type: ignore[index]


def _row_int(row: object, key: str, index: int, name: str) -> int:
    value = _row_value(row, key, index)
    if isinstance(value, bool) or not isinstance(value, int):
        raise FeatureStoreError(f"{name} returned by database must be an integer")
    return value


def _row_str(row: object, key: str, index: int, name: str) -> str:
    return _non_empty_str(_row_value(row, key, index), name)


def _optional_row_str(row: object, key: str, index: int, name: str) -> str | None:
    value = _row_value(row, key, index)
    if value is None:
        return None
    return _non_empty_str(value, name)


def _row_date(row: object, key: str, index: int, name: str) -> date:
    value = _row_value(row, key, index)
    if isinstance(value, datetime) or not isinstance(value, date):
        raise FeatureStoreError(f"{name} returned by database must be a date")
    return value


def _optional_row_date(row: object, key: str, index: int, name: str) -> date | None:
    value = _row_value(row, key, index)
    if value is None:
        return None
    if isinstance(value, datetime) or not isinstance(value, date):
        raise FeatureStoreError(f"{name} returned by database must be a date")
    return value


def _row_datetime(row: object, key: str, index: int, name: str) -> datetime:
    value = _row_value(row, key, index)
    if not isinstance(value, datetime):
        raise FeatureStoreError(f"{name} returned by database must be a datetime")
    _require_aware(value, name)
    return value


def _optional_row_datetime(
    row: object,
    key: str,
    index: int,
    name: str,
) -> datetime | None:
    value = _row_value(row, key, index)
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise FeatureStoreError(f"{name} returned by database must be a datetime")
    _require_aware(value, name)
    return value


def _row_bool(row: object, key: str, index: int, name: str) -> bool:
    value = _row_value(row, key, index)
    if not isinstance(value, bool):
        raise FeatureStoreError(f"{name} returned by database must be a boolean")
    return value


def _row_decimal(row: object, key: str, index: int, name: str) -> Decimal:
    value = _row_value(row, key, index)
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, (str, int)):
        result = Decimal(str(value))
    else:
        raise FeatureStoreError(f"{name} returned by database must be numeric")
    if not result.is_finite() or result <= 0:
        raise FeatureStoreError(f"{name} returned by database must be positive")
    return result


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


_INSERT_FEATURE_DEFINITION_SQL = """
INSERT INTO silver.feature_definitions (
    name,
    version,
    kind,
    computation_spec,
    definition_hash,
    notes
) VALUES (
    %(name)s,
    %(version)s,
    %(kind)s,
    %(computation_spec)s::jsonb,
    %(definition_hash)s,
    %(notes)s
)
ON CONFLICT (name, version) DO NOTHING
RETURNING id, name, version, kind, computation_spec, definition_hash, notes;
""".strip()

_SELECT_FEATURE_DEFINITION_SQL = """
SELECT id, name, version, kind, computation_spec, definition_hash, notes
FROM silver.feature_definitions
WHERE name = %(name)s
  AND version = %(version)s
LIMIT 1;
""".strip()

_SELECT_POLICY_BY_NAME_VERSION_SQL = """
SELECT id, name, version, rule
FROM silver.available_at_policies
WHERE name = %(name)s
  AND version = %(version)s
LIMIT 1;
""".strip()

_SELECT_UNIVERSE_MEMBERSHIPS_SQL = """
SELECT
    membership.security_id,
    security.ticker,
    membership.valid_from,
    membership.valid_to
FROM silver.universe_membership AS membership
JOIN silver.securities AS security
    ON security.id = membership.security_id
WHERE membership.universe_name = %(universe_name)s
  AND (%(end_date)s::date IS NULL OR membership.valid_from <= %(end_date)s::date)
  AND (
      %(start_date)s::date IS NULL
      OR membership.valid_to IS NULL
      OR membership.valid_to >= %(start_date)s::date
  )
ORDER BY membership.security_id, membership.valid_from;
""".strip()

_SELECT_TRADING_CALENDAR_SQL = """
SELECT date, is_session, session_close, is_early_close
FROM silver.trading_calendar
WHERE %(end_date)s::date IS NULL OR date <= %(end_date)s::date
ORDER BY date;
""".strip()

_SELECT_ADJUSTED_PRICES_SQL = """
SELECT security_id, date, adj_close, available_at
FROM silver.prices_daily
WHERE security_id = ANY(%(security_ids)s)
  AND available_at_policy_id = %(available_at_policy_id)s
  AND (%(end_date)s::date IS NULL OR date <= %(end_date)s::date)
ORDER BY security_id, date;
""".strip()

_UPSERT_FEATURE_VALUE_SQL = """
INSERT INTO silver.feature_values (
    security_id,
    asof_date,
    feature_definition_id,
    value,
    available_at,
    available_at_policy_id,
    computed_by_run_id,
    source_metadata
) VALUES (
    %(security_id)s,
    %(asof_date)s,
    %(feature_definition_id)s,
    %(value)s,
    %(available_at)s,
    %(available_at_policy_id)s,
    %(computed_by_run_id)s,
    %(source_metadata)s::jsonb
)
ON CONFLICT (security_id, asof_date, feature_definition_id) DO UPDATE SET
    value = EXCLUDED.value,
    available_at = EXCLUDED.available_at,
    available_at_policy_id = EXCLUDED.available_at_policy_id,
    computed_by_run_id = EXCLUDED.computed_by_run_id,
    computed_at = now(),
    source_metadata = EXCLUDED.source_metadata
WHERE
    silver.feature_values.value IS DISTINCT FROM EXCLUDED.value
    OR silver.feature_values.available_at IS DISTINCT FROM EXCLUDED.available_at
    OR silver.feature_values.available_at_policy_id IS DISTINCT FROM
        EXCLUDED.available_at_policy_id
    OR silver.feature_values.computed_by_run_id IS DISTINCT FROM
        EXCLUDED.computed_by_run_id
    OR silver.feature_values.source_metadata IS DISTINCT FROM
        EXCLUDED.source_metadata;
""".strip()

_INSERT_ANALYTICS_RUN_SQL = """
INSERT INTO silver.analytics_runs (
    run_kind,
    code_git_sha,
    feature_set_hash,
    available_at_policy_versions,
    parameters,
    input_fingerprints,
    status
) VALUES (
    'feature_generation',
    %(code_git_sha)s,
    %(feature_set_hash)s,
    %(available_at_policy_versions)s::jsonb,
    %(parameters)s::jsonb,
    %(input_fingerprints)s::jsonb,
    'running'
)
RETURNING id;
""".strip()

_FINISH_ANALYTICS_RUN_SQL = """
UPDATE silver.analytics_runs
SET status = %(status)s,
    finished_at = now()
WHERE id = %(run_id)s;
""".strip()
