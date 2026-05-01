"""Persistence helpers for Silver hypothesis candidates and evaluations."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any


HYPOTHESIS_STATUSES = frozenset(
    ("proposed", "running", "rejected", "promising", "accepted", "retired")
)
EVALUATION_STATUSES = frozenset(
    ("running", "rejected", "promising", "accepted", "failed")
)
HORIZONS = frozenset((5, 21, 63, 126, 252))
TARGET_KINDS = frozenset(
    (
        "raw_return",
        "excess_return",
        "excess_return_market",
        "excess_return_sector",
        "risk_adjusted_return",
    )
)


class HypothesisRegistryError(ValueError):
    """Raised when hypothesis registry inputs or rows are invalid."""


@dataclass(frozen=True, slots=True)
class HypothesisCreate:
    hypothesis_key: str
    name: str
    thesis: str
    signal_name: str
    mechanism: str
    universe_name: str | None = None
    horizon_days: int | None = None
    target_kind: str | None = None
    status: str = "proposed"
    metadata: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class HypothesisRecord:
    id: int
    hypothesis_key: str
    name: str
    thesis: str
    signal_name: str
    mechanism: str
    universe_name: str | None
    horizon_days: int | None
    target_kind: str | None
    status: str
    metadata: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class HypothesisEvaluationRecord:
    id: int
    hypothesis_id: int
    model_run_id: int
    backtest_run_id: int
    evaluation_status: str
    failure_reason: str | None
    notes: str | None
    summary_metrics: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class HypothesisSummary:
    id: int
    hypothesis_key: str
    name: str
    status: str
    signal_name: str
    universe_name: str | None
    horizon_days: int | None
    target_kind: str | None
    latest_evaluation_status: str | None
    latest_failure_reason: str | None
    latest_backtest_run_id: int | None
    latest_backtest_run_key: str | None


@dataclass(frozen=True, slots=True)
class _BacktestEvaluationSource:
    id: int
    backtest_run_key: str
    model_run_id: int
    status: str
    label_scramble_pass: bool | None
    metrics: Mapping[str, Any]


class HypothesisRepository:
    """Read and write hypothesis registry rows.

    The repository owns SQL shape and validation, while callers own transaction
    boundaries. This keeps it usable from scripts and tests without forcing a
    specific connection lifecycle.
    """

    def __init__(self, connection: Any):
        self._connection = connection

    def upsert_hypothesis(self, hypothesis: HypothesisCreate) -> HypothesisRecord:
        params = _hypothesis_params(hypothesis)
        with _cursor(self._connection) as cursor:
            cursor.execute(_UPSERT_HYPOTHESIS_SQL, params)
            row = cursor.fetchone()
        if row is None:
            raise HypothesisRegistryError("hypothesis upsert returned no row")
        return _hypothesis_record(row)

    def load_hypothesis(self, hypothesis_key: str) -> HypothesisRecord:
        key = _key(hypothesis_key, "hypothesis_key")
        with _cursor(self._connection) as cursor:
            cursor.execute(_SELECT_HYPOTHESIS_BY_KEY_SQL, {"hypothesis_key": key})
            row = cursor.fetchone()
        if row is None:
            raise HypothesisRegistryError(f"hypothesis {key} was not found")
        return _hypothesis_record(row)

    def record_latest_falsifier_evaluation(
        self,
        *,
        hypothesis_key: str,
        strategy: str,
        universe_name: str,
        horizon_days: int,
        evaluation_status: str | None = None,
        failure_reason: str | None = None,
        notes: str | None = None,
    ) -> HypothesisEvaluationRecord:
        strategy_name = _label(strategy, "strategy")
        universe = _label(universe_name, "universe_name")
        horizon = _horizon(horizon_days)
        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_LATEST_FALSIFIER_BACKTEST_SQL,
                {
                    "strategy": strategy_name,
                    "universe_name": universe,
                    "horizon_days": horizon,
                },
            )
            row = cursor.fetchone()
        if row is None:
            raise HypothesisRegistryError(
                "no succeeded falsifier backtest was found for "
                f"{strategy_name} {universe} h{horizon}"
            )
        return self.record_backtest_evaluation(
            hypothesis_key=hypothesis_key,
            backtest_run_id=_row_int(row, "id", 0, "backtest_runs.id"),
            evaluation_status=evaluation_status,
            failure_reason=failure_reason,
            notes=notes,
        )

    def record_backtest_evaluation(
        self,
        *,
        hypothesis_key: str,
        backtest_run_id: int,
        evaluation_status: str | None = None,
        failure_reason: str | None = None,
        notes: str | None = None,
    ) -> HypothesisEvaluationRecord:
        hypothesis = self.load_hypothesis(hypothesis_key)
        source = self._load_backtest_source(backtest_run_id)
        status, inferred_failure = _evaluation_status(
            source,
            explicit_status=evaluation_status,
            explicit_failure_reason=failure_reason,
        )
        params = {
            "hypothesis_id": hypothesis.id,
            "model_run_id": source.model_run_id,
            "backtest_run_id": source.id,
            "evaluation_status": status,
            "failure_reason": _optional_label(
                failure_reason or inferred_failure,
                "failure_reason",
            ),
            "notes": _optional_label(notes, "notes"),
            "summary_metrics": _json_dumps(
                {
                    "backtest_status": source.status,
                    "label_scramble_pass": source.label_scramble_pass,
                    "metrics": dict(source.metrics),
                },
                "summary_metrics",
            ),
        }
        with _cursor(self._connection) as cursor:
            cursor.execute(_UPSERT_HYPOTHESIS_EVALUATION_SQL, params)
            row = cursor.fetchone()
            hypothesis_status = _hypothesis_status_for_evaluation(status)
            if hypothesis_status is not None and hypothesis_status != hypothesis.status:
                cursor.execute(
                    _UPDATE_HYPOTHESIS_STATUS_SQL,
                    {
                        "hypothesis_id": hypothesis.id,
                        "status": hypothesis_status,
                    },
                )
        if row is None:
            raise HypothesisRegistryError("hypothesis evaluation upsert returned no row")
        return _evaluation_record(row)

    def list_hypotheses(self) -> tuple[HypothesisSummary, ...]:
        with _cursor(self._connection) as cursor:
            cursor.execute(_SELECT_HYPOTHESIS_SUMMARIES_SQL, {})
            rows = cursor.fetchall()
        return tuple(_hypothesis_summary(row) for row in rows)

    def _load_backtest_source(self, backtest_run_id: int) -> _BacktestEvaluationSource:
        normalized_id = _positive_int(backtest_run_id, "backtest_run_id")
        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_BACKTEST_EVALUATION_SOURCE_SQL,
                {"backtest_run_id": normalized_id},
            )
            row = cursor.fetchone()
        if row is None:
            raise HypothesisRegistryError(
                f"backtest_run_id {normalized_id} was not found"
            )
        return _backtest_source(row)


def momentum_12_1_hypothesis() -> HypothesisCreate:
    """Return the seed hypothesis represented by the Phase 1 falsifier."""
    return HypothesisCreate(
        hypothesis_key="momentum_12_1",
        name="Momentum 12-1",
        thesis=(
            "Securities with stronger prior 12-month returns, skipping the most "
            "recent month, may continue to outperform over the next quarter."
        ),
        signal_name="momentum_12_1",
        mechanism=(
            "Trend persistence and delayed investor reaction can make medium-term "
            "relative strength informative after costs."
        ),
        universe_name="falsifier_seed",
        horizon_days=63,
        target_kind="raw_return",
        status="proposed",
        metadata={
            "seed_source": "silver_phase1_falsifier",
            "strategy": "momentum_12_1",
        },
    )


def _hypothesis_params(hypothesis: HypothesisCreate) -> dict[str, Any]:
    return {
        "hypothesis_key": _key(hypothesis.hypothesis_key, "hypothesis_key"),
        "name": _label(hypothesis.name, "name"),
        "thesis": _label(hypothesis.thesis, "thesis"),
        "signal_name": _label(hypothesis.signal_name, "signal_name"),
        "mechanism": _label(hypothesis.mechanism, "mechanism"),
        "universe_name": _optional_label(hypothesis.universe_name, "universe_name"),
        "horizon_days": (
            None if hypothesis.horizon_days is None else _horizon(hypothesis.horizon_days)
        ),
        "target_kind": _optional_target_kind(hypothesis.target_kind),
        "status": _hypothesis_status(hypothesis.status),
        "metadata": _json_dumps(hypothesis.metadata or {}, "metadata"),
    }


def _evaluation_status(
    source: _BacktestEvaluationSource,
    *,
    explicit_status: str | None,
    explicit_failure_reason: str | None,
) -> tuple[str, str | None]:
    if explicit_status is not None:
        return _evaluation_status_label(explicit_status), explicit_failure_reason
    if source.status == "running":
        return "running", None
    if source.status == "failed":
        return "failed", explicit_failure_reason or "backtest_failed"
    if source.status == "insufficient_data":
        return "rejected", explicit_failure_reason or "insufficient_data"
    if source.status == "succeeded" and source.label_scramble_pass is False:
        return "rejected", explicit_failure_reason or "label_scramble_failed"
    if source.status == "succeeded" and source.label_scramble_pass is True:
        return "promising", None
    return "failed", explicit_failure_reason or f"unsupported_backtest_status:{source.status}"


def _hypothesis_record(row: object) -> HypothesisRecord:
    return HypothesisRecord(
        id=_row_int(row, "id", 0, "hypotheses.id"),
        hypothesis_key=_row_str(row, "hypothesis_key", 1, "hypothesis_key"),
        name=_row_str(row, "name", 2, "name"),
        thesis=_row_str(row, "thesis", 3, "thesis"),
        signal_name=_row_str(row, "signal_name", 4, "signal_name"),
        mechanism=_row_str(row, "mechanism", 5, "mechanism"),
        universe_name=_row_optional_str(row, "universe_name", 6),
        horizon_days=_row_optional_int(row, "horizon_days", 7, "horizon_days"),
        target_kind=_row_optional_str(row, "target_kind", 8),
        status=_row_str(row, "status", 9, "status"),
        metadata=_row_mapping(row, "metadata", 10, "metadata"),
    )


def _evaluation_record(row: object) -> HypothesisEvaluationRecord:
    return HypothesisEvaluationRecord(
        id=_row_int(row, "id", 0, "hypothesis_evaluations.id"),
        hypothesis_id=_row_int(row, "hypothesis_id", 1, "hypothesis_id"),
        model_run_id=_row_int(row, "model_run_id", 2, "model_run_id"),
        backtest_run_id=_row_int(row, "backtest_run_id", 3, "backtest_run_id"),
        evaluation_status=_row_str(row, "evaluation_status", 4, "evaluation_status"),
        failure_reason=_row_optional_str(row, "failure_reason", 5),
        notes=_row_optional_str(row, "notes", 6),
        summary_metrics=_row_mapping(row, "summary_metrics", 7, "summary_metrics"),
    )


def _hypothesis_summary(row: object) -> HypothesisSummary:
    return HypothesisSummary(
        id=_row_int(row, "id", 0, "hypotheses.id"),
        hypothesis_key=_row_str(row, "hypothesis_key", 1, "hypothesis_key"),
        name=_row_str(row, "name", 2, "name"),
        status=_row_str(row, "status", 3, "status"),
        signal_name=_row_str(row, "signal_name", 4, "signal_name"),
        universe_name=_row_optional_str(row, "universe_name", 5),
        horizon_days=_row_optional_int(row, "horizon_days", 6, "horizon_days"),
        target_kind=_row_optional_str(row, "target_kind", 7),
        latest_evaluation_status=_row_optional_str(
            row,
            "latest_evaluation_status",
            8,
        ),
        latest_failure_reason=_row_optional_str(row, "latest_failure_reason", 9),
        latest_backtest_run_id=_row_optional_int(
            row,
            "latest_backtest_run_id",
            10,
            "latest_backtest_run_id",
        ),
        latest_backtest_run_key=_row_optional_str(row, "latest_backtest_run_key", 11),
    )


def _backtest_source(row: object) -> _BacktestEvaluationSource:
    return _BacktestEvaluationSource(
        id=_row_int(row, "id", 0, "backtest_runs.id"),
        backtest_run_key=_row_str(row, "backtest_run_key", 1, "backtest_run_key"),
        model_run_id=_row_int(row, "model_run_id", 2, "model_run_id"),
        status=_row_str(row, "status", 3, "backtest_runs.status"),
        label_scramble_pass=_row_optional_bool(row, "label_scramble_pass", 4),
        metrics=_row_mapping(row, "metrics", 5, "backtest_runs.metrics"),
    )


def _key(value: str, name: str) -> str:
    normalized = _label(value, name)
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,127}", normalized):
        raise HypothesisRegistryError(
            f"{name} must start with a lowercase letter or digit and contain "
            "only lowercase letters, digits, underscores, or dashes"
        )
    return normalized


def _label(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HypothesisRegistryError(f"{name} must be a non-empty string")
    return value.strip()


def _optional_label(value: str | None, name: str) -> str | None:
    if value is None:
        return None
    return _label(value, name)


def _horizon(value: int) -> int:
    normalized = _positive_int(value, "horizon_days")
    if normalized not in HORIZONS:
        allowed = ", ".join(str(horizon) for horizon in sorted(HORIZONS))
        raise HypothesisRegistryError(f"horizon_days must be one of {allowed}")
    return normalized


def _hypothesis_status(value: str) -> str:
    status = _label(value, "hypothesis status")
    if status not in HYPOTHESIS_STATUSES:
        allowed = ", ".join(sorted(HYPOTHESIS_STATUSES))
        raise HypothesisRegistryError(f"hypothesis status must be one of {allowed}")
    return status


def _evaluation_status_label(value: str) -> str:
    status = _label(value, "evaluation_status")
    if status not in EVALUATION_STATUSES:
        allowed = ", ".join(sorted(EVALUATION_STATUSES))
        raise HypothesisRegistryError(f"evaluation_status must be one of {allowed}")
    return status


def _hypothesis_status_for_evaluation(evaluation_status: str) -> str | None:
    if evaluation_status in {"running", "rejected", "promising", "accepted"}:
        return evaluation_status
    return None


def _optional_target_kind(value: str | None) -> str | None:
    target_kind = _optional_label(value, "target_kind")
    if target_kind is not None and target_kind not in TARGET_KINDS:
        allowed = ", ".join(sorted(TARGET_KINDS))
        raise HypothesisRegistryError(f"target_kind must be one of {allowed}")
    return target_kind


def _json_dumps(value: Mapping[str, Any], name: str) -> str:
    if not isinstance(value, Mapping):
        raise HypothesisRegistryError(f"{name} must be a mapping")
    return json.dumps(dict(value), sort_keys=True, separators=(",", ":"))


def _positive_int(value: int, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise HypothesisRegistryError(f"{name} must be a positive integer")
    return value


def _row_mapping(row: object, key: str, index: int, name: str) -> Mapping[str, Any]:
    value = _row_value(row, key, index)
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, Mapping):
        raise HypothesisRegistryError(f"{name} returned by database must be an object")
    return dict(value)


def _row_str(row: object, key: str, index: int, name: str) -> str:
    value = _row_value(row, key, index)
    if not isinstance(value, str) or not value.strip():
        raise HypothesisRegistryError(f"{name} returned by database must be a string")
    return value.strip()


def _row_optional_str(row: object, key: str, index: int) -> str | None:
    value = _row_value(row, key, index)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise HypothesisRegistryError(f"{key} returned by database must be a string")
    return value.strip()


def _row_int(row: object, key: str, index: int, name: str) -> int:
    value = _row_value(row, key, index)
    if not isinstance(value, int) or isinstance(value, bool):
        raise HypothesisRegistryError(f"{name} returned by database must be an integer")
    return value


def _row_optional_int(row: object, key: str, index: int, name: str) -> int | None:
    value = _row_value(row, key, index)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise HypothesisRegistryError(f"{name} returned by database must be an integer")
    return value


def _row_optional_bool(row: object, key: str, index: int) -> bool | None:
    value = _row_value(row, key, index)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise HypothesisRegistryError(f"{key} returned by database must be a boolean")
    return value


def _row_value(row: object, key: str, index: int) -> object:
    if isinstance(row, Mapping):
        return row[key]
    return row[index]  # type: ignore[index]


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
        if callable(close):
            close()


_HYPOTHESIS_RETURNING_COLUMNS = """
    id,
    hypothesis_key,
    name,
    thesis,
    signal_name,
    mechanism,
    universe_name,
    horizon_days,
    target_kind,
    status,
    metadata
"""

_EVALUATION_RETURNING_COLUMNS = """
    id,
    hypothesis_id,
    model_run_id,
    backtest_run_id,
    evaluation_status,
    failure_reason,
    notes,
    summary_metrics
"""

_UPSERT_HYPOTHESIS_SQL = f"""
INSERT INTO silver.hypotheses (
    hypothesis_key,
    name,
    thesis,
    signal_name,
    mechanism,
    universe_name,
    horizon_days,
    target_kind,
    status,
    metadata
)
VALUES (
    %(hypothesis_key)s,
    %(name)s,
    %(thesis)s,
    %(signal_name)s,
    %(mechanism)s,
    %(universe_name)s,
    %(horizon_days)s,
    %(target_kind)s,
    %(status)s,
    %(metadata)s::jsonb
)
ON CONFLICT (hypothesis_key) DO UPDATE SET
    name = EXCLUDED.name,
    thesis = EXCLUDED.thesis,
    signal_name = EXCLUDED.signal_name,
    mechanism = EXCLUDED.mechanism,
    universe_name = EXCLUDED.universe_name,
    horizon_days = EXCLUDED.horizon_days,
    target_kind = EXCLUDED.target_kind,
    metadata = EXCLUDED.metadata,
    updated_at = now()
RETURNING {_HYPOTHESIS_RETURNING_COLUMNS};
"""

_SELECT_HYPOTHESIS_BY_KEY_SQL = f"""
SELECT {_HYPOTHESIS_RETURNING_COLUMNS}
FROM silver.hypotheses
WHERE hypothesis_key = %(hypothesis_key)s;
"""

_SELECT_BACKTEST_EVALUATION_SOURCE_SQL = """
SELECT
    br.id,
    br.backtest_run_key,
    br.model_run_id,
    br.status,
    br.label_scramble_pass,
    br.metrics
FROM silver.backtest_runs AS br
WHERE br.id = %(backtest_run_id)s;
"""

_SELECT_LATEST_FALSIFIER_BACKTEST_SQL = """
SELECT
    id
FROM silver.backtest_runs
WHERE status = 'succeeded'
  AND parameters->>'strategy' = %(strategy)s
  AND universe_name = %(universe_name)s
  AND horizon_days = %(horizon_days)s
ORDER BY id DESC
LIMIT 1;
"""

_UPSERT_HYPOTHESIS_EVALUATION_SQL = f"""
INSERT INTO silver.hypothesis_evaluations (
    hypothesis_id,
    model_run_id,
    backtest_run_id,
    evaluation_status,
    failure_reason,
    notes,
    summary_metrics
)
VALUES (
    %(hypothesis_id)s,
    %(model_run_id)s,
    %(backtest_run_id)s,
    %(evaluation_status)s,
    %(failure_reason)s,
    %(notes)s,
    %(summary_metrics)s::jsonb
)
ON CONFLICT (hypothesis_id, backtest_run_id) DO UPDATE SET
    model_run_id = EXCLUDED.model_run_id,
    evaluation_status = EXCLUDED.evaluation_status,
    failure_reason = EXCLUDED.failure_reason,
    notes = EXCLUDED.notes,
    summary_metrics = EXCLUDED.summary_metrics,
    updated_at = now()
RETURNING {_EVALUATION_RETURNING_COLUMNS};
"""

_UPDATE_HYPOTHESIS_STATUS_SQL = """
UPDATE silver.hypotheses
SET
    status = %(status)s,
    updated_at = now()
WHERE id = %(hypothesis_id)s;
"""

_SELECT_HYPOTHESIS_SUMMARIES_SQL = """
SELECT
    h.id,
    h.hypothesis_key,
    h.name,
    h.status,
    h.signal_name,
    h.universe_name,
    h.horizon_days,
    h.target_kind,
    latest.evaluation_status AS latest_evaluation_status,
    latest.failure_reason AS latest_failure_reason,
    latest.backtest_run_id AS latest_backtest_run_id,
    br.backtest_run_key AS latest_backtest_run_key
FROM silver.hypotheses AS h
LEFT JOIN LATERAL (
    SELECT
        evaluation_status,
        failure_reason,
        backtest_run_id
    FROM silver.hypothesis_evaluations AS he
    WHERE he.hypothesis_id = h.id
    ORDER BY he.created_at DESC, he.id DESC
    LIMIT 1
) AS latest ON true
LEFT JOIN silver.backtest_runs AS br
    ON br.id = latest.backtest_run_id
ORDER BY h.hypothesis_key;
"""
