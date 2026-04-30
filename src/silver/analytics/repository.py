"""Persistence helpers for analytics run lineage rows."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any


class AnalyticsRunError(ValueError):
    """Raised when analytics run lineage cannot be written safely."""


class BacktestMetadataError(ValueError):
    """Raised when backtest metadata writes would violate the registry contract."""


@dataclass(frozen=True, slots=True)
class AnalyticsRunRecord:
    """Minimal analytics run metadata returned by repository writes."""

    id: int
    run_kind: str
    status: str


@dataclass(frozen=True, slots=True)
class ModelRunCreate:
    """Validated payload for creating a ``silver.model_runs`` row."""

    model_run_key: str
    name: str
    code_git_sha: str
    feature_set_hash: str
    training_start_date: date
    training_end_date: date
    test_start_date: date
    test_end_date: date
    horizon_days: int
    target_kind: str
    random_seed: int
    feature_snapshot_ref: str | None = None
    cost_assumptions: Mapping[str, Any] = field(default_factory=dict)
    parameters: Mapping[str, Any] = field(default_factory=dict)
    available_at_policy_versions: Mapping[str, Any] = field(default_factory=dict)
    input_fingerprints: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ModelRunFinish:
    """Validated payload for finishing a ``silver.model_runs`` row."""

    status: str
    metrics: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ModelRunRecord:
    """Stable identity returned from model-run repository writes."""

    id: int
    model_run_key: str
    status: str


@dataclass(frozen=True, slots=True)
class BacktestRunCreate:
    """Validated payload for creating a ``silver.backtest_runs`` row."""

    backtest_run_key: str
    model_run_id: int
    name: str
    universe_name: str
    horizon_days: int
    target_kind: str
    cost_assumptions: Mapping[str, Any] = field(default_factory=dict)
    parameters: Mapping[str, Any] = field(default_factory=dict)
    multiple_comparisons_correction: str | None = None


@dataclass(frozen=True, slots=True)
class BacktestRunFinish:
    """Validated payload for finishing a ``silver.backtest_runs`` row."""

    status: str
    cost_assumptions: Mapping[str, Any] = field(default_factory=dict)
    metrics: Mapping[str, Any] = field(default_factory=dict)
    metrics_by_regime: Mapping[str, Any] = field(default_factory=dict)
    baseline_metrics: Mapping[str, Any] = field(default_factory=dict)
    label_scramble_metrics: Mapping[str, Any] = field(default_factory=dict)
    label_scramble_pass: bool | None = None
    multiple_comparisons_correction: str | None = None


@dataclass(frozen=True, slots=True)
class BacktestRunRecord:
    """Stable identity returned from backtest-run repository writes."""

    id: int
    backtest_run_key: str
    status: str


@dataclass(frozen=True, slots=True)
class BacktestTraceabilitySnapshot:
    """Joined model/backtest metadata used to audit a reported run identity."""

    model_run_id: int
    model_run_key: str
    model_status: str
    model_code_git_sha: str
    model_feature_set_hash: str
    model_feature_snapshot_ref: str | None
    model_training_start_date: date
    model_training_end_date: date
    model_test_start_date: date
    model_test_end_date: date
    model_horizon_days: int
    model_target_kind: str
    model_random_seed: int
    model_cost_assumptions: Mapping[str, Any]
    model_metrics: Mapping[str, Any]
    model_parameters: Mapping[str, Any]
    model_available_at_policy_versions: Mapping[str, Any]
    model_input_fingerprints: Mapping[str, Any]
    backtest_run_id: int
    backtest_run_key: str
    backtest_status: str
    backtest_model_run_id: int
    backtest_universe_name: str
    backtest_horizon_days: int
    backtest_target_kind: str
    backtest_cost_assumptions: Mapping[str, Any]
    backtest_metrics: Mapping[str, Any]
    backtest_metrics_by_regime: Mapping[str, Any]
    backtest_baseline_metrics: Mapping[str, Any]
    backtest_label_scramble_metrics: Mapping[str, Any]
    backtest_label_scramble_pass: bool | None
    backtest_parameters: Mapping[str, Any]
    backtest_multiple_comparisons_correction: str | None


@dataclass(frozen=True, slots=True)
class ModelRunReplayMetadata:
    """Replay-critical metadata loaded directly from ``silver.model_runs``."""

    model_run_id: int
    model_run_key: str
    status: str
    code_git_sha: str
    feature_set_hash: str
    feature_snapshot_ref: str | None
    training_start_date: date
    training_end_date: date
    test_start_date: date
    test_end_date: date
    horizon_days: int
    target_kind: str
    random_seed: int
    cost_assumptions: Mapping[str, Any]
    metrics: Mapping[str, Any]
    parameters: Mapping[str, Any]
    available_at_policy_versions: Mapping[str, Any]
    input_fingerprints: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class BacktestReplayComparison:
    """Field-level comparison of stored and replayed backtest claim metadata."""

    stored_backtest_run_id: int
    stored_backtest_run_key: str
    replayed_backtest_run_id: int
    replayed_backtest_run_key: str
    mismatches: tuple[str, ...]

    @property
    def matches(self) -> bool:
        """Return whether every replay-critical field matched."""
        return not self.mismatches


def compare_backtest_replay_snapshots(
    stored: BacktestTraceabilitySnapshot,
    replayed: BacktestTraceabilitySnapshot,
) -> BacktestReplayComparison:
    """Compare replay-critical model/backtest metadata field by field."""
    if not isinstance(stored, BacktestTraceabilitySnapshot):
        raise BacktestMetadataError("stored must be a BacktestTraceabilitySnapshot")
    if not isinstance(replayed, BacktestTraceabilitySnapshot):
        raise BacktestMetadataError("replayed must be a BacktestTraceabilitySnapshot")

    mismatches: list[str] = []
    for field_name, attribute in _REPLAY_SNAPSHOT_COMPARISON_FIELDS:
        _expect_replay_value(
            mismatches,
            field_name,
            expected=getattr(stored, attribute),
            actual=getattr(replayed, attribute),
        )
    return BacktestReplayComparison(
        stored_backtest_run_id=stored.backtest_run_id,
        stored_backtest_run_key=stored.backtest_run_key,
        replayed_backtest_run_id=replayed.backtest_run_id,
        replayed_backtest_run_key=replayed.backtest_run_key,
        mismatches=tuple(mismatches),
    )


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


class BacktestMetadataRepository:
    """Write model-run and backtest-run metadata rows.

    The repository owns SQL shape and validation but not transaction boundaries.
    Callers provide a DB-API compatible connection and decide when to commit.
    """

    def __init__(self, connection: Any):
        self._connection = connection

    def create_model_run(self, run: ModelRunCreate) -> ModelRunRecord:
        """Create or load a deterministic model-run row by stable key."""
        params = _model_run_create_params(run)
        with _cursor(self._connection) as cursor:
            cursor.execute(_INSERT_MODEL_RUN_SQL, params)
            row = cursor.fetchone()
        if row is not None:
            return _model_run_record(row)

        existing = self._load_model_run_by_key(params["model_run_key"])
        if not _model_run_matches_create_params(existing, params):
            raise BacktestMetadataError(
                f"model_run_key {params['model_run_key']} already exists with "
                "different metadata"
            )
        return _model_run_record(existing)

    def finish_model_run(
        self,
        run_id: int,
        finish: ModelRunFinish,
    ) -> ModelRunRecord:
        """Finish a running model run with an allowed terminal status."""
        params = _model_run_finish_params(run_id, finish)
        with _cursor(self._connection) as cursor:
            cursor.execute(_FINISH_MODEL_RUN_SQL, params)
            row = cursor.fetchone()
        if row is None:
            raise BacktestMetadataError(
                f"model run {params['run_id']} was not found or is not running"
            )
        return _model_run_record(row)

    def create_backtest_run(self, run: BacktestRunCreate) -> BacktestRunRecord:
        """Create or load a deterministic backtest-run row by stable key."""
        params = _backtest_run_create_params(run)
        with _cursor(self._connection) as cursor:
            cursor.execute(_INSERT_BACKTEST_RUN_SQL, params)
            row = cursor.fetchone()
        if row is not None:
            return _backtest_run_record(row)

        existing = self._load_backtest_run_by_key(params["backtest_run_key"])
        if not _backtest_run_matches_create_params(existing, params):
            raise BacktestMetadataError(
                f"backtest_run_key {params['backtest_run_key']} already exists "
                "with different metadata"
            )
        return _backtest_run_record(existing)

    def finish_backtest_run(
        self,
        run_id: int,
        finish: BacktestRunFinish,
    ) -> BacktestRunRecord:
        """Finish a running backtest run with metrics and reproducibility metadata."""
        params = _backtest_run_finish_params(run_id, finish)
        with _cursor(self._connection) as cursor:
            cursor.execute(_FINISH_BACKTEST_RUN_SQL, params)
            row = cursor.fetchone()
        if row is None:
            raise BacktestMetadataError(
                f"backtest run {params['run_id']} was not found or is not running"
            )
        return _backtest_run_record(row)

    def load_backtest_traceability_snapshot(
        self,
        backtest_run_id: int,
    ) -> BacktestTraceabilitySnapshot:
        """Load the joined model/backtest metadata for a reported backtest run."""
        normalized_id = _metadata_positive_int(backtest_run_id, "backtest_run_id")
        try:
            return self.load_backtest_replay_snapshot(backtest_run_id=normalized_id)
        except BacktestMetadataError as exc:
            if "replay metadata was not found" not in str(exc):
                raise
            raise BacktestMetadataError(
                f"backtest run {normalized_id} traceability metadata was not found"
            ) from exc

    def load_model_run_replay_metadata(
        self,
        *,
        model_run_id: int | None = None,
        model_run_key: str | None = None,
    ) -> ModelRunReplayMetadata:
        """Load replay-critical ``model_runs`` metadata by durable id or key."""
        if (model_run_id is None) == (model_run_key is None):
            raise BacktestMetadataError("exactly one model identity must be supplied")

        if model_run_id is not None:
            params = {
                "model_run_id": _metadata_positive_int(
                    model_run_id,
                    "model_run_id",
                ),
            }
            sql = _SELECT_MODEL_REPLAY_BY_ID_SQL
            missing = f"model_run_id {params['model_run_id']}"
        else:
            params = {
                "model_run_key": _metadata_label(
                    model_run_key,
                    "model_run_key",
                ),
            }
            sql = _SELECT_MODEL_REPLAY_BY_KEY_SQL
            missing = f"model_run_key {params['model_run_key']}"

        with _cursor(self._connection) as cursor:
            cursor.execute(sql, params)
            row = cursor.fetchone()
        if row is None:
            raise BacktestMetadataError(f"{missing} replay metadata was not found")
        return _model_run_replay_metadata(row)

    def load_backtest_replay_snapshot(
        self,
        *,
        backtest_run_id: int | None = None,
        backtest_run_key: str | None = None,
    ) -> BacktestTraceabilitySnapshot:
        """Load joined replay metadata for an accepted backtest claim candidate."""
        if (backtest_run_id is None) == (backtest_run_key is None):
            raise BacktestMetadataError(
                "exactly one backtest identity must be supplied"
            )

        if backtest_run_id is not None:
            params = {
                "backtest_run_id": _metadata_positive_int(
                    backtest_run_id,
                    "backtest_run_id",
                ),
            }
            sql = _SELECT_BACKTEST_TRACEABILITY_BY_ID_SQL
            missing = f"backtest_run_id {params['backtest_run_id']}"
        else:
            params = {
                "backtest_run_key": _metadata_label(
                    backtest_run_key,
                    "backtest_run_key",
                ),
            }
            sql = _SELECT_BACKTEST_TRACEABILITY_BY_KEY_SQL
            missing = f"backtest_run_key {params['backtest_run_key']}"

        with _cursor(self._connection) as cursor:
            cursor.execute(sql, params)
            row = cursor.fetchone()
        if row is None:
            raise BacktestMetadataError(f"{missing} replay metadata was not found")
        return _backtest_traceability_snapshot(row)


    def _load_model_run_by_key(self, model_run_key: str) -> object:
        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_MODEL_RUN_BY_KEY_SQL,
                {"model_run_key": model_run_key},
            )
            row = cursor.fetchone()
        if row is None:
            raise BacktestMetadataError(
                f"model_run_key {model_run_key} conflicted but could not be loaded"
            )
        return row

    def _load_backtest_run_by_key(self, backtest_run_key: str) -> object:
        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_BACKTEST_RUN_BY_KEY_SQL,
                {"backtest_run_key": backtest_run_key},
            )
            row = cursor.fetchone()
        if row is None:
            raise BacktestMetadataError(
                f"backtest_run_key {backtest_run_key} conflicted but could not "
                "be loaded"
            )
        return row


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


_CODE_SHA_RE = re.compile(r"^[0-9a-f]{7,64}$")
_FEATURE_SET_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_ALLOWED_HORIZONS = {5, 21, 63, 126, 252}
_ALLOWED_TARGET_KINDS = {
    "raw_return",
    "excess_return",
    "excess_return_market",
    "excess_return_sector",
    "risk_adjusted_return",
}
_TERMINAL_STATUSES = {"succeeded", "failed", "insufficient_data"}
_ALLOWED_CORRECTIONS = {"bh", "bonferroni", "none"}


def _model_run_create_params(run: ModelRunCreate) -> dict[str, Any]:
    if not isinstance(run, ModelRunCreate):
        raise BacktestMetadataError("run must be a ModelRunCreate")

    training_start = _metadata_date(run.training_start_date, "training_start_date")
    training_end = _metadata_date(run.training_end_date, "training_end_date")
    test_start = _metadata_date(run.test_start_date, "test_start_date")
    test_end = _metadata_date(run.test_end_date, "test_end_date")
    if training_end < training_start:
        raise BacktestMetadataError(
            "training_end_date must be on or after training_start_date"
        )
    if test_end < test_start:
        raise BacktestMetadataError("test_end_date must be on or after test_start_date")
    if test_start <= training_end:
        raise BacktestMetadataError("test_start_date must be after training_end_date")

    feature_snapshot_ref = _optional_metadata_label(
        run.feature_snapshot_ref,
        "feature_snapshot_ref",
    )
    cost_assumptions = _json_object(run.cost_assumptions, "cost_assumptions")
    if not cost_assumptions:
        raise BacktestMetadataError("cost_assumptions must be non-empty")
    available_at_policy_versions = _json_object(
        run.available_at_policy_versions,
        "available_at_policy_versions",
    )
    if not available_at_policy_versions:
        raise BacktestMetadataError(
            "available_at_policy_versions must be non-empty"
        )
    input_fingerprints = _json_object(run.input_fingerprints, "input_fingerprints")
    if feature_snapshot_ref is None and not input_fingerprints:
        raise BacktestMetadataError(
            "model runs must provide feature_snapshot_ref or input_fingerprints"
        )

    return {
        "model_run_key": _metadata_label(run.model_run_key, "model_run_key"),
        "name": _metadata_label(run.name, "name"),
        "code_git_sha": _code_git_sha(run.code_git_sha),
        "feature_set_hash": _feature_set_hash(run.feature_set_hash),
        "feature_snapshot_ref": feature_snapshot_ref,
        "training_start_date": training_start,
        "training_end_date": training_end,
        "test_start_date": test_start,
        "test_end_date": test_end,
        "horizon_days": _horizon_days(run.horizon_days),
        "target_kind": _target_kind(run.target_kind),
        "random_seed": _non_negative_int(run.random_seed, "random_seed"),
        "cost_assumptions": _json_dumps_object(
            cost_assumptions,
            "cost_assumptions",
        ),
        "parameters": _json_dumps_object(run.parameters, "parameters"),
        "available_at_policy_versions": _json_dumps_object(
            available_at_policy_versions,
            "available_at_policy_versions",
        ),
        "input_fingerprints": _json_dumps_object(
            input_fingerprints,
            "input_fingerprints",
        ),
    }


def _model_run_finish_params(
    run_id: int,
    finish: ModelRunFinish,
) -> dict[str, Any]:
    if not isinstance(finish, ModelRunFinish):
        raise BacktestMetadataError("finish must be a ModelRunFinish")
    normalized_status = _terminal_status(finish.status)
    metrics = _json_object(finish.metrics, "metrics")
    if normalized_status == "succeeded" and not metrics:
        raise BacktestMetadataError("metrics must be non-empty for succeeded model runs")
    return {
        "run_id": _metadata_positive_int(run_id, "run_id"),
        "status": normalized_status,
        "metrics": _json_dumps_object(metrics, "metrics"),
    }


def _backtest_run_create_params(run: BacktestRunCreate) -> dict[str, Any]:
    if not isinstance(run, BacktestRunCreate):
        raise BacktestMetadataError("run must be a BacktestRunCreate")
    cost_assumptions = _json_object(run.cost_assumptions, "cost_assumptions")
    if not cost_assumptions:
        raise BacktestMetadataError("cost_assumptions must be non-empty")
    return {
        "backtest_run_key": _metadata_label(
            run.backtest_run_key,
            "backtest_run_key",
        ),
        "model_run_id": _metadata_positive_int(run.model_run_id, "model_run_id"),
        "name": _metadata_label(run.name, "name"),
        "universe_name": _metadata_label(run.universe_name, "universe_name"),
        "horizon_days": _horizon_days(run.horizon_days),
        "target_kind": _target_kind(run.target_kind),
        "cost_assumptions": _json_dumps_object(
            cost_assumptions,
            "cost_assumptions",
        ),
        "parameters": _json_dumps_object(run.parameters, "parameters"),
        "multiple_comparisons_correction": _optional_correction(
            run.multiple_comparisons_correction,
        ),
    }


def _backtest_run_finish_params(
    run_id: int,
    finish: BacktestRunFinish,
) -> dict[str, Any]:
    if not isinstance(finish, BacktestRunFinish):
        raise BacktestMetadataError("finish must be a BacktestRunFinish")
    normalized_status = _terminal_status(finish.status)
    cost_assumptions = _json_object(finish.cost_assumptions, "cost_assumptions")
    metrics = _json_object(finish.metrics, "metrics")
    metrics_by_regime = _json_object(finish.metrics_by_regime, "metrics_by_regime")
    baseline_metrics = _json_object(finish.baseline_metrics, "baseline_metrics")
    label_scramble_metrics = _json_object(
        finish.label_scramble_metrics,
        "label_scramble_metrics",
    )
    if normalized_status == "succeeded":
        if not metrics:
            raise BacktestMetadataError(
                "metrics must be non-empty for succeeded backtest runs"
            )
        if not metrics_by_regime:
            raise BacktestMetadataError(
                "metrics_by_regime must be non-empty for succeeded backtest runs"
            )
        if not baseline_metrics:
            raise BacktestMetadataError(
                "baseline_metrics must be non-empty for succeeded backtest runs"
            )
        if not label_scramble_metrics:
            raise BacktestMetadataError(
                "label_scramble_metrics must be non-empty for succeeded backtest runs"
            )
        if not cost_assumptions:
            raise BacktestMetadataError(
                "cost_assumptions must be non-empty for succeeded backtest runs"
            )
    if normalized_status in {"succeeded", "insufficient_data"}:
        _metadata_bool(finish.label_scramble_pass, "label_scramble_pass")
    elif finish.label_scramble_pass is not None:
        _metadata_bool(finish.label_scramble_pass, "label_scramble_pass")

    return {
        "run_id": _metadata_positive_int(run_id, "run_id"),
        "status": normalized_status,
        "cost_assumptions": _json_dumps_object(
            cost_assumptions,
            "cost_assumptions",
        ),
        "metrics": _json_dumps_object(metrics, "metrics"),
        "metrics_by_regime": _json_dumps_object(
            metrics_by_regime,
            "metrics_by_regime",
        ),
        "baseline_metrics": _json_dumps_object(
            baseline_metrics,
            "baseline_metrics",
        ),
        "label_scramble_metrics": _json_dumps_object(
            label_scramble_metrics,
            "label_scramble_metrics",
        ),
        "label_scramble_pass": finish.label_scramble_pass,
        "multiple_comparisons_correction": _optional_correction(
            finish.multiple_comparisons_correction,
        ),
    }


def _model_run_record(row: object) -> ModelRunRecord:
    return ModelRunRecord(
        id=_metadata_row_int(row, "id", 0, "model_runs.id"),
        model_run_key=_metadata_row_str(
            row,
            "model_run_key",
            1,
            "model_runs.model_run_key",
        ),
        status=_metadata_row_str(row, "status", 2, "model_runs.status"),
    )


def _backtest_run_record(row: object) -> BacktestRunRecord:
    return BacktestRunRecord(
        id=_metadata_row_int(row, "id", 0, "backtest_runs.id"),
        backtest_run_key=_metadata_row_str(
            row,
            "backtest_run_key",
            1,
            "backtest_runs.backtest_run_key",
        ),
        status=_metadata_row_str(row, "status", 2, "backtest_runs.status"),
    )


def _backtest_traceability_snapshot(row: object) -> BacktestTraceabilitySnapshot:
    return BacktestTraceabilitySnapshot(
        model_run_id=_metadata_row_int(row, "model_run_id", 0, "model_runs.id"),
        model_run_key=_metadata_row_str(
            row,
            "model_run_key",
            1,
            "model_runs.model_run_key",
        ),
        model_status=_metadata_row_str(row, "model_status", 2, "model_runs.status"),
        model_code_git_sha=_metadata_row_str(
            row,
            "model_code_git_sha",
            3,
            "model_runs.code_git_sha",
        ),
        model_feature_set_hash=_metadata_row_str(
            row,
            "model_feature_set_hash",
            4,
            "model_runs.feature_set_hash",
        ),
        model_feature_snapshot_ref=_metadata_row_optional_str(
            row,
            "model_feature_snapshot_ref",
            5,
            "model_runs.feature_snapshot_ref",
        ),
        model_training_start_date=_metadata_row_date(
            row,
            "model_training_start_date",
            6,
            "model_runs.training_start_date",
        ),
        model_training_end_date=_metadata_row_date(
            row,
            "model_training_end_date",
            7,
            "model_runs.training_end_date",
        ),
        model_test_start_date=_metadata_row_date(
            row,
            "model_test_start_date",
            8,
            "model_runs.test_start_date",
        ),
        model_test_end_date=_metadata_row_date(
            row,
            "model_test_end_date",
            9,
            "model_runs.test_end_date",
        ),
        model_horizon_days=_metadata_row_int(
            row,
            "model_horizon_days",
            10,
            "model_runs.horizon_days",
        ),
        model_target_kind=_metadata_row_str(
            row,
            "model_target_kind",
            11,
            "model_runs.target_kind",
        ),
        model_random_seed=_metadata_row_int(
            row,
            "model_random_seed",
            12,
            "model_runs.random_seed",
        ),
        model_cost_assumptions=_metadata_row_json_object(
            row,
            "model_cost_assumptions",
            13,
            "model_runs.cost_assumptions",
        ),
        model_metrics=_metadata_row_json_object(
            row,
            "model_metrics",
            14,
            "model_runs.metrics",
        ),
        model_parameters=_metadata_row_json_object(
            row,
            "model_parameters",
            15,
            "model_runs.parameters",
        ),
        model_available_at_policy_versions=_metadata_row_json_object(
            row,
            "model_available_at_policy_versions",
            16,
            "model_runs.available_at_policy_versions",
        ),
        model_input_fingerprints=_metadata_row_json_object(
            row,
            "model_input_fingerprints",
            17,
            "model_runs.input_fingerprints",
        ),
        backtest_run_id=_metadata_row_int(
            row,
            "backtest_run_id",
            18,
            "backtest_runs.id",
        ),
        backtest_run_key=_metadata_row_str(
            row,
            "backtest_run_key",
            19,
            "backtest_runs.backtest_run_key",
        ),
        backtest_status=_metadata_row_str(
            row,
            "backtest_status",
            20,
            "backtest_runs.status",
        ),
        backtest_model_run_id=_metadata_row_int(
            row,
            "backtest_model_run_id",
            21,
            "backtest_runs.model_run_id",
        ),
        backtest_universe_name=_metadata_row_str(
            row,
            "backtest_universe_name",
            22,
            "backtest_runs.universe_name",
        ),
        backtest_horizon_days=_metadata_row_int(
            row,
            "backtest_horizon_days",
            23,
            "backtest_runs.horizon_days",
        ),
        backtest_target_kind=_metadata_row_str(
            row,
            "backtest_target_kind",
            24,
            "backtest_runs.target_kind",
        ),
        backtest_cost_assumptions=_metadata_row_json_object(
            row,
            "backtest_cost_assumptions",
            25,
            "backtest_runs.cost_assumptions",
        ),
        backtest_metrics=_metadata_row_json_object(
            row,
            "backtest_metrics",
            26,
            "backtest_runs.metrics",
        ),
        backtest_metrics_by_regime=_metadata_row_json_object(
            row,
            "backtest_metrics_by_regime",
            27,
            "backtest_runs.metrics_by_regime",
        ),
        backtest_baseline_metrics=_metadata_row_json_object(
            row,
            "backtest_baseline_metrics",
            28,
            "backtest_runs.baseline_metrics",
        ),
        backtest_label_scramble_metrics=_metadata_row_json_object(
            row,
            "backtest_label_scramble_metrics",
            29,
            "backtest_runs.label_scramble_metrics",
        ),
        backtest_label_scramble_pass=_metadata_row_optional_bool(
            row,
            "backtest_label_scramble_pass",
            30,
            "backtest_runs.label_scramble_pass",
        ),
        backtest_parameters=_metadata_row_json_object(
            row,
            "backtest_parameters",
            31,
            "backtest_runs.parameters",
        ),
        backtest_multiple_comparisons_correction=_metadata_row_optional_str(
            row,
            "backtest_multiple_comparisons_correction",
            32,
            "backtest_runs.multiple_comparisons_correction",
        ),
    )


def _model_run_replay_metadata(row: object) -> ModelRunReplayMetadata:
    return ModelRunReplayMetadata(
        model_run_id=_metadata_row_int(row, "model_run_id", 0, "model_runs.id"),
        model_run_key=_metadata_row_str(
            row,
            "model_run_key",
            1,
            "model_runs.model_run_key",
        ),
        status=_metadata_row_str(row, "status", 2, "model_runs.status"),
        code_git_sha=_metadata_row_str(
            row,
            "code_git_sha",
            3,
            "model_runs.code_git_sha",
        ),
        feature_set_hash=_metadata_row_str(
            row,
            "feature_set_hash",
            4,
            "model_runs.feature_set_hash",
        ),
        feature_snapshot_ref=_metadata_row_optional_str(
            row,
            "feature_snapshot_ref",
            5,
            "model_runs.feature_snapshot_ref",
        ),
        training_start_date=_metadata_row_date(
            row,
            "training_start_date",
            6,
            "model_runs.training_start_date",
        ),
        training_end_date=_metadata_row_date(
            row,
            "training_end_date",
            7,
            "model_runs.training_end_date",
        ),
        test_start_date=_metadata_row_date(
            row,
            "test_start_date",
            8,
            "model_runs.test_start_date",
        ),
        test_end_date=_metadata_row_date(
            row,
            "test_end_date",
            9,
            "model_runs.test_end_date",
        ),
        horizon_days=_metadata_row_int(
            row,
            "horizon_days",
            10,
            "model_runs.horizon_days",
        ),
        target_kind=_metadata_row_str(
            row,
            "target_kind",
            11,
            "model_runs.target_kind",
        ),
        random_seed=_metadata_row_int(
            row,
            "random_seed",
            12,
            "model_runs.random_seed",
        ),
        cost_assumptions=_metadata_row_json_object(
            row,
            "cost_assumptions",
            13,
            "model_runs.cost_assumptions",
        ),
        metrics=_metadata_row_json_object(
            row,
            "metrics",
            14,
            "model_runs.metrics",
        ),
        parameters=_metadata_row_json_object(
            row,
            "parameters",
            15,
            "model_runs.parameters",
        ),
        available_at_policy_versions=_metadata_row_json_object(
            row,
            "available_at_policy_versions",
            16,
            "model_runs.available_at_policy_versions",
        ),
        input_fingerprints=_metadata_row_json_object(
            row,
            "input_fingerprints",
            17,
            "model_runs.input_fingerprints",
        ),
    )


def _model_run_matches_create_params(row: object, params: Mapping[str, Any]) -> bool:
    return (
        _metadata_row_str(row, "name", 3, "model_runs.name") == params["name"]
        and _metadata_row_str(row, "code_git_sha", 4, "model_runs.code_git_sha")
        == params["code_git_sha"]
        and _metadata_row_str(
            row,
            "feature_set_hash",
            5,
            "model_runs.feature_set_hash",
        )
        == params["feature_set_hash"]
        and _metadata_row_optional_str(
            row,
            "feature_snapshot_ref",
            6,
            "model_runs.feature_snapshot_ref",
        )
        == params["feature_snapshot_ref"]
        and _metadata_row_date(
            row,
            "training_start_date",
            7,
            "model_runs.training_start_date",
        )
        == params["training_start_date"]
        and _metadata_row_date(
            row,
            "training_end_date",
            8,
            "model_runs.training_end_date",
        )
        == params["training_end_date"]
        and _metadata_row_date(row, "test_start_date", 9, "model_runs.test_start_date")
        == params["test_start_date"]
        and _metadata_row_date(row, "test_end_date", 10, "model_runs.test_end_date")
        == params["test_end_date"]
        and _metadata_row_int(row, "horizon_days", 11, "model_runs.horizon_days")
        == params["horizon_days"]
        and _metadata_row_str(row, "target_kind", 12, "model_runs.target_kind")
        == params["target_kind"]
        and _metadata_row_int(row, "random_seed", 13, "model_runs.random_seed")
        == params["random_seed"]
        and _row_json_matches_param(
            row,
            "cost_assumptions",
            14,
            params["cost_assumptions"],
            "model_runs.cost_assumptions",
        )
        and _row_json_matches_param(
            row,
            "parameters",
            15,
            params["parameters"],
            "model_runs.parameters",
        )
        and _row_json_matches_param(
            row,
            "available_at_policy_versions",
            16,
            params["available_at_policy_versions"],
            "model_runs.available_at_policy_versions",
        )
        and _row_json_matches_param(
            row,
            "input_fingerprints",
            17,
            params["input_fingerprints"],
            "model_runs.input_fingerprints",
        )
    )


def _backtest_run_matches_create_params(row: object, params: Mapping[str, Any]) -> bool:
    return (
        _metadata_row_int(row, "model_run_id", 3, "backtest_runs.model_run_id")
        == params["model_run_id"]
        and _metadata_row_str(row, "name", 4, "backtest_runs.name")
        == params["name"]
        and _metadata_row_str(row, "universe_name", 5, "backtest_runs.universe_name")
        == params["universe_name"]
        and _metadata_row_int(row, "horizon_days", 6, "backtest_runs.horizon_days")
        == params["horizon_days"]
        and _metadata_row_str(row, "target_kind", 7, "backtest_runs.target_kind")
        == params["target_kind"]
        and _row_json_matches_param(
            row,
            "cost_assumptions",
            8,
            params["cost_assumptions"],
            "backtest_runs.cost_assumptions",
        )
        and _row_json_matches_param(
            row,
            "parameters",
            9,
            params["parameters"],
            "backtest_runs.parameters",
        )
        and _metadata_row_optional_str(
            row,
            "multiple_comparisons_correction",
            10,
            "backtest_runs.multiple_comparisons_correction",
        )
        == params["multiple_comparisons_correction"]
    )


def _metadata_label(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BacktestMetadataError(f"{name} must be a non-empty string")
    return value.strip()


def _optional_metadata_label(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _metadata_label(value, name)


def _code_git_sha(value: object) -> str:
    normalized = _metadata_label(value, "code_git_sha")
    if _CODE_SHA_RE.fullmatch(normalized) is None:
        raise BacktestMetadataError(
            "code_git_sha must be 7 to 64 lowercase hex characters"
        )
    return normalized


def _feature_set_hash(value: object) -> str:
    normalized = _metadata_label(value, "feature_set_hash")
    if _FEATURE_SET_HASH_RE.fullmatch(normalized) is None:
        raise BacktestMetadataError(
            "feature_set_hash must be 64 lowercase hex characters"
        )
    return normalized


def _metadata_positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise BacktestMetadataError(f"{name} must be a positive integer")
    return value


def _non_negative_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BacktestMetadataError(f"{name} must be a non-negative integer")
    return value


def _horizon_days(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BacktestMetadataError("horizon_days must be an integer")
    if value not in _ALLOWED_HORIZONS:
        raise BacktestMetadataError("horizon_days must be one of 5, 21, 63, 126, 252")
    return value


def _target_kind(value: object) -> str:
    normalized = _metadata_label(value, "target_kind").lower()
    if normalized not in _ALLOWED_TARGET_KINDS:
        allowed = ", ".join(sorted(_ALLOWED_TARGET_KINDS))
        raise BacktestMetadataError(f"target_kind must be one of {allowed}")
    return normalized


def _terminal_status(value: object) -> str:
    normalized = _metadata_label(value, "status").lower()
    if normalized not in _TERMINAL_STATUSES:
        raise BacktestMetadataError(
            "status must be succeeded, failed, or insufficient_data"
        )
    return normalized


def _optional_correction(value: object) -> str | None:
    if value is None:
        return None
    normalized = _metadata_label(value, "multiple_comparisons_correction").lower()
    if normalized not in _ALLOWED_CORRECTIONS:
        raise BacktestMetadataError(
            "multiple_comparisons_correction must be bh, bonferroni, or none"
        )
    return normalized


def _metadata_date(value: object, name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise BacktestMetadataError(f"{name} must be a date")
    return value


def _metadata_bool(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise BacktestMetadataError(f"{name} must be a boolean")
    return value


def _json_dumps_object(value: object, name: str) -> str:
    return json.dumps(
        _json_object(value, name),
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_object(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise BacktestMetadataError(f"{name} must be a JSON object")
    normalized = _json_normalize(value)
    if not isinstance(normalized, dict):
        raise BacktestMetadataError(f"{name} must be a JSON object")
    return normalized


def _json_normalize(value: object) -> Any:
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise BacktestMetadataError("JSON object keys must be non-empty strings")
            normalized[key] = _json_normalize(item)
        return normalized
    if isinstance(value, tuple):
        return [_json_normalize(item) for item in value]
    if isinstance(value, list):
        return [_json_normalize(item) for item in value]
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise BacktestMetadataError("datetime JSON values must be timezone-aware")
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise BacktestMetadataError("Decimal JSON values must be finite")
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise BacktestMetadataError("float JSON values must be finite")
        return value
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    raise BacktestMetadataError(f"value is not JSON serializable: {value!r}")


def _json_loads_if_needed(value: object, name: str) -> object:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise BacktestMetadataError(f"{name} returned invalid JSON") from exc
    return value


def _row_json_matches_param(
    row: object,
    key: str,
    index: int,
    param_json: str,
    name: str,
) -> bool:
    return _json_object(
        _json_loads_if_needed(_row_value(row, key, index), name),
        name,
    ) == json.loads(param_json)


def _metadata_row_int(row: object, key: str, index: int, name: str) -> int:
    value = _row_value(row, key, index)
    if isinstance(value, bool) or not isinstance(value, int):
        raise BacktestMetadataError(f"{name} returned by database must be an integer")
    return value


def _metadata_row_str(row: object, key: str, index: int, name: str) -> str:
    value = _row_value(row, key, index)
    if not isinstance(value, str) or not value.strip():
        raise BacktestMetadataError(f"{name} returned by database must be a string")
    return value.strip()


def _metadata_row_optional_str(
    row: object,
    key: str,
    index: int,
    name: str,
) -> str | None:
    value = _row_value(row, key, index)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise BacktestMetadataError(
            f"{name} returned by database must be a string or null"
        )
    return value.strip()


def _metadata_row_date(row: object, key: str, index: int, name: str) -> date:
    value = _row_value(row, key, index)
    if isinstance(value, datetime) or not isinstance(value, date):
        raise BacktestMetadataError(f"{name} returned by database must be a date")
    return value


def _metadata_row_optional_bool(
    row: object,
    key: str,
    index: int,
    name: str,
) -> bool | None:
    value = _row_value(row, key, index)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise BacktestMetadataError(
            f"{name} returned by database must be a boolean or null"
        )
    return value


def _metadata_row_json_object(
    row: object,
    key: str,
    index: int,
    name: str,
) -> dict[str, Any]:
    return _json_object(
        _json_loads_if_needed(_row_value(row, key, index), name),
        name,
    )


def _metadata_row_optional_bool(
    row: object,
    key: str,
    index: int,
    name: str,
) -> bool | None:
    value = _row_value(row, key, index)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise BacktestMetadataError(
            f"{name} returned by database must be a boolean or null"
        )
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


_REPLAY_SNAPSHOT_COMPARISON_FIELDS = (
    ("model_runs.model_run_key", "model_run_key"),
    ("model_runs.status", "model_status"),
    ("model_runs.code_git_sha", "model_code_git_sha"),
    ("model_runs.feature_set_hash", "model_feature_set_hash"),
    ("model_runs.feature_snapshot_ref", "model_feature_snapshot_ref"),
    ("model_runs.training_start_date", "model_training_start_date"),
    ("model_runs.training_end_date", "model_training_end_date"),
    ("model_runs.test_start_date", "model_test_start_date"),
    ("model_runs.test_end_date", "model_test_end_date"),
    ("model_runs.horizon_days", "model_horizon_days"),
    ("model_runs.target_kind", "model_target_kind"),
    ("model_runs.random_seed", "model_random_seed"),
    ("model_runs.cost_assumptions", "model_cost_assumptions"),
    ("model_runs.metrics", "model_metrics"),
    ("model_runs.parameters", "model_parameters"),
    (
        "model_runs.available_at_policy_versions",
        "model_available_at_policy_versions",
    ),
    ("model_runs.input_fingerprints", "model_input_fingerprints"),
    ("backtest_runs.backtest_run_key", "backtest_run_key"),
    ("backtest_runs.model_run_id", "backtest_model_run_id"),
    ("backtest_runs.status", "backtest_status"),
    ("backtest_runs.universe_name", "backtest_universe_name"),
    ("backtest_runs.horizon_days", "backtest_horizon_days"),
    ("backtest_runs.target_kind", "backtest_target_kind"),
    ("backtest_runs.cost_assumptions", "backtest_cost_assumptions"),
    ("backtest_runs.parameters", "backtest_parameters"),
    ("backtest_runs.metrics", "backtest_metrics"),
    ("backtest_runs.metrics_by_regime", "backtest_metrics_by_regime"),
    ("backtest_runs.baseline_metrics", "backtest_baseline_metrics"),
    ("backtest_runs.label_scramble_metrics", "backtest_label_scramble_metrics"),
    ("backtest_runs.label_scramble_pass", "backtest_label_scramble_pass"),
    (
        "backtest_runs.multiple_comparisons_correction",
        "backtest_multiple_comparisons_correction",
    ),
)


def _expect_replay_value(
    mismatches: list[str],
    field: str,
    *,
    expected: object,
    actual: object,
) -> None:
    normalized_expected = _replay_normalize(expected)
    normalized_actual = _replay_normalize(actual)
    if normalized_actual != normalized_expected:
        mismatches.append(
            f"{field} expected {_replay_token(normalized_expected)} "
            f"got {_replay_token(normalized_actual)}"
        )


def _replay_normalize(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            str(key): _replay_normalize(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, tuple):
        return [_replay_normalize(item) for item in value]
    if isinstance(value, list):
        return [_replay_normalize(item) for item in value]
    if isinstance(value, date):
        return value.isoformat()
    return value


def _replay_token(value: object) -> str:
    if isinstance(value, (Mapping, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return repr(value)


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

_INSERT_MODEL_RUN_SQL = """
INSERT INTO silver.model_runs (
    model_run_key,
    name,
    code_git_sha,
    feature_set_hash,
    feature_snapshot_ref,
    training_start_date,
    training_end_date,
    test_start_date,
    test_end_date,
    horizon_days,
    target_kind,
    random_seed,
    cost_assumptions,
    parameters,
    available_at_policy_versions,
    input_fingerprints,
    status
) VALUES (
    %(model_run_key)s,
    %(name)s,
    %(code_git_sha)s,
    %(feature_set_hash)s,
    %(feature_snapshot_ref)s,
    %(training_start_date)s,
    %(training_end_date)s,
    %(test_start_date)s,
    %(test_end_date)s,
    %(horizon_days)s,
    %(target_kind)s,
    %(random_seed)s,
    %(cost_assumptions)s::jsonb,
    %(parameters)s::jsonb,
    %(available_at_policy_versions)s::jsonb,
    %(input_fingerprints)s::jsonb,
    'running'
)
ON CONFLICT (model_run_key) DO NOTHING
RETURNING id, model_run_key, status;
""".strip()

_SELECT_MODEL_RUN_BY_KEY_SQL = """
SELECT
    id,
    model_run_key,
    status,
    name,
    code_git_sha,
    feature_set_hash,
    feature_snapshot_ref,
    training_start_date,
    training_end_date,
    test_start_date,
    test_end_date,
    horizon_days,
    target_kind,
    random_seed,
    cost_assumptions,
    parameters,
    available_at_policy_versions,
    input_fingerprints
FROM silver.model_runs
WHERE model_run_key = %(model_run_key)s
LIMIT 1;
""".strip()

_SELECT_MODEL_REPLAY_BY_ID_SQL = """
SELECT
    id AS model_run_id,
    model_run_key,
    status,
    code_git_sha,
    feature_set_hash,
    feature_snapshot_ref,
    training_start_date,
    training_end_date,
    test_start_date,
    test_end_date,
    horizon_days,
    target_kind,
    random_seed,
    cost_assumptions,
    metrics,
    parameters,
    available_at_policy_versions,
    input_fingerprints
FROM silver.model_runs
WHERE id = %(model_run_id)s
LIMIT 1;
""".strip()

_SELECT_MODEL_REPLAY_BY_KEY_SQL = """
SELECT
    id AS model_run_id,
    model_run_key,
    status,
    code_git_sha,
    feature_set_hash,
    feature_snapshot_ref,
    training_start_date,
    training_end_date,
    test_start_date,
    test_end_date,
    horizon_days,
    target_kind,
    random_seed,
    cost_assumptions,
    metrics,
    parameters,
    available_at_policy_versions,
    input_fingerprints
FROM silver.model_runs
WHERE model_run_key = %(model_run_key)s
LIMIT 1;
""".strip()

_FINISH_MODEL_RUN_SQL = """
UPDATE silver.model_runs
SET status = %(status)s,
    finished_at = now(),
    metrics = %(metrics)s::jsonb
WHERE id = %(run_id)s
  AND status = 'running'
RETURNING id, model_run_key, status;
""".strip()

_INSERT_BACKTEST_RUN_SQL = """
INSERT INTO silver.backtest_runs (
    backtest_run_key,
    model_run_id,
    name,
    universe_name,
    horizon_days,
    target_kind,
    cost_assumptions,
    parameters,
    multiple_comparisons_correction,
    status
) VALUES (
    %(backtest_run_key)s,
    %(model_run_id)s,
    %(name)s,
    %(universe_name)s,
    %(horizon_days)s,
    %(target_kind)s,
    %(cost_assumptions)s::jsonb,
    %(parameters)s::jsonb,
    %(multiple_comparisons_correction)s,
    'running'
)
ON CONFLICT (backtest_run_key) DO NOTHING
RETURNING id, backtest_run_key, status;
""".strip()

_SELECT_BACKTEST_RUN_BY_KEY_SQL = """
SELECT
    id,
    backtest_run_key,
    status,
    model_run_id,
    name,
    universe_name,
    horizon_days,
    target_kind,
    cost_assumptions,
    parameters,
    multiple_comparisons_correction
FROM silver.backtest_runs
WHERE backtest_run_key = %(backtest_run_key)s
LIMIT 1;
""".strip()

_SELECT_BACKTEST_TRACEABILITY_COLUMNS = """
SELECT
    mr.id AS model_run_id,
    mr.model_run_key,
    mr.status AS model_status,
    mr.code_git_sha AS model_code_git_sha,
    mr.feature_set_hash AS model_feature_set_hash,
    mr.feature_snapshot_ref AS model_feature_snapshot_ref,
    mr.training_start_date AS model_training_start_date,
    mr.training_end_date AS model_training_end_date,
    mr.test_start_date AS model_test_start_date,
    mr.test_end_date AS model_test_end_date,
    mr.horizon_days AS model_horizon_days,
    mr.target_kind AS model_target_kind,
    mr.random_seed AS model_random_seed,
    mr.cost_assumptions AS model_cost_assumptions,
    mr.metrics AS model_metrics,
    mr.parameters AS model_parameters,
    mr.available_at_policy_versions AS model_available_at_policy_versions,
    mr.input_fingerprints AS model_input_fingerprints,
    br.id AS backtest_run_id,
    br.backtest_run_key,
    br.status AS backtest_status,
    br.model_run_id AS backtest_model_run_id,
    br.universe_name AS backtest_universe_name,
    br.horizon_days AS backtest_horizon_days,
    br.target_kind AS backtest_target_kind,
    br.cost_assumptions AS backtest_cost_assumptions,
    br.metrics AS backtest_metrics,
    br.metrics_by_regime AS backtest_metrics_by_regime,
    br.baseline_metrics AS backtest_baseline_metrics,
    br.label_scramble_metrics AS backtest_label_scramble_metrics,
    br.label_scramble_pass AS backtest_label_scramble_pass,
    br.parameters AS backtest_parameters,
    br.multiple_comparisons_correction AS backtest_multiple_comparisons_correction
FROM silver.backtest_runs br
JOIN silver.model_runs mr ON mr.id = br.model_run_id
""".strip()

_SELECT_BACKTEST_TRACEABILITY_BY_ID_SQL = (
    _SELECT_BACKTEST_TRACEABILITY_COLUMNS
    + """
WHERE br.id = %(backtest_run_id)s
LIMIT 1;
""".rstrip()
)

_SELECT_BACKTEST_TRACEABILITY_BY_KEY_SQL = (
    _SELECT_BACKTEST_TRACEABILITY_COLUMNS
    + """
WHERE br.backtest_run_key = %(backtest_run_key)s
LIMIT 1;
""".rstrip()
)

_FINISH_BACKTEST_RUN_SQL = """
UPDATE silver.backtest_runs
SET status = %(status)s,
    finished_at = now(),
    cost_assumptions = %(cost_assumptions)s::jsonb,
    metrics = %(metrics)s::jsonb,
    metrics_by_regime = %(metrics_by_regime)s::jsonb,
    baseline_metrics = %(baseline_metrics)s::jsonb,
    label_scramble_metrics = %(label_scramble_metrics)s::jsonb,
    label_scramble_pass = %(label_scramble_pass)s,
    multiple_comparisons_correction = %(multiple_comparisons_correction)s
WHERE id = %(run_id)s
  AND status = 'running'
RETURNING id, backtest_run_key, status;
""".strip()
