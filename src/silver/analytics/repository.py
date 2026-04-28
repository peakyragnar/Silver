"""Persistence helpers for analytics run lineage rows."""

from __future__ import annotations

import json
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any


class AnalyticsRunError(ValueError):
    """Raised when analytics run lineage cannot be written safely."""


@dataclass(frozen=True, slots=True)
class AnalyticsRunRecord:
    """Minimal analytics run metadata returned by repository writes."""

    id: int
    run_kind: str
    status: str


class AnalyticsRunRepository:
    """Write and finish rows in ``silver.analytics_runs``."""

    def __init__(self, connection: Any):
        self._connection = connection

    def create_run(
        self,
        *,
        run_kind: str,
        code_git_sha: str,
        available_at_policy_versions: Mapping[str, Any] | None = None,
        parameters: Mapping[str, Any] | None = None,
        input_fingerprints: Mapping[str, Any] | None = None,
        random_seed: int | None = None,
    ) -> AnalyticsRunRecord:
        """Create a running analytics lineage row."""
        params = {
            "run_kind": _required_label(run_kind, "run_kind"),
            "code_git_sha": _required_label(code_git_sha, "code_git_sha"),
            "available_at_policy_versions": _stable_json(
                _mapping(available_at_policy_versions, "available_at_policy_versions")
            ),
            "parameters": _stable_json(_mapping(parameters, "parameters")),
            "input_fingerprints": _stable_json(
                _mapping(input_fingerprints, "input_fingerprints")
            ),
            "random_seed": _optional_int(random_seed, "random_seed"),
        }
        with _cursor(self._connection) as cursor:
            cursor.execute(_INSERT_ANALYTICS_RUN_SQL, params)
            row = cursor.fetchone()
        if row is None:
            raise AnalyticsRunError("analytics run insert did not return a row")
        return _run_record(row)

    def finish_run(self, run_id: int, *, status: str) -> AnalyticsRunRecord:
        """Mark a running analytics row as succeeded or failed."""
        normalized_status = _required_label(status, "status").lower()
        if normalized_status not in {"succeeded", "failed"}:
            raise AnalyticsRunError("status must be succeeded or failed")
        with _cursor(self._connection) as cursor:
            cursor.execute(
                _FINISH_ANALYTICS_RUN_SQL,
                {
                    "run_id": _positive_int(run_id, "run_id"),
                    "status": normalized_status,
                },
            )
            row = cursor.fetchone()
        if row is None:
            raise AnalyticsRunError(f"analytics run {run_id} was not found")
        return _run_record(row)


def _mapping(value: Mapping[str, Any] | None, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise AnalyticsRunError(f"{name} must be a mapping")
    return dict(value)


def _stable_json(value: Mapping[str, Any]) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    except TypeError as exc:
        raise AnalyticsRunError(
            "analytics run metadata must be JSON serializable"
        ) from exc


def _required_label(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AnalyticsRunError(f"{name} must be a non-empty string")
    return value.strip()


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise AnalyticsRunError(f"{name} must be a positive integer")
    return value


def _optional_int(value: object, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise AnalyticsRunError(f"{name} must be an integer")
    return value


def _run_record(row: object) -> AnalyticsRunRecord:
    return AnalyticsRunRecord(
        id=_row_int(row, "id", 0, "analytics_runs.id"),
        run_kind=_row_str(row, "run_kind", 1, "analytics_runs.run_kind"),
        status=_row_str(row, "status", 2, "analytics_runs.status"),
    )


def _row_value(row: object, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return row[index]  # type: ignore[index]


def _row_int(row: object, key: str, index: int, name: str) -> int:
    value = _row_value(row, key, index)
    if isinstance(value, bool) or not isinstance(value, int):
        raise AnalyticsRunError(f"{name} returned by database must be an integer")
    return value


def _row_str(row: object, key: str, index: int, name: str) -> str:
    value = _row_value(row, key, index)
    if not isinstance(value, str) or not value.strip():
        raise AnalyticsRunError(f"{name} returned by database must be a string")
    return value.strip()


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


_INSERT_ANALYTICS_RUN_SQL = """
INSERT INTO silver.analytics_runs (
    run_kind,
    code_git_sha,
    available_at_policy_versions,
    parameters,
    input_fingerprints,
    random_seed
) VALUES (
    %(run_kind)s,
    %(code_git_sha)s,
    %(available_at_policy_versions)s::jsonb,
    %(parameters)s::jsonb,
    %(input_fingerprints)s::jsonb,
    %(random_seed)s
)
RETURNING id, run_kind, status;
""".strip()

_FINISH_ANALYTICS_RUN_SQL = """
UPDATE silver.analytics_runs
SET status = %(status)s,
    finished_at = now()
WHERE id = %(run_id)s
RETURNING id, run_kind, status;
""".strip()
