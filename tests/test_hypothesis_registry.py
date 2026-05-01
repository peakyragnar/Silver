from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest

from silver.hypotheses import (
    HypothesisRegistryError,
    HypothesisRepository,
    HypothesisSummary,
    momentum_12_1_hypothesis,
)


ROOT = Path(__file__).resolve().parents[1]
MANAGE_HYPOTHESES_SCRIPT = ROOT / "scripts" / "manage_hypotheses.py"


def load_manage_hypotheses_module():
    spec = importlib.util.spec_from_file_location(
        "manage_hypotheses",
        MANAGE_HYPOTHESES_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


manage_hypotheses = load_manage_hypotheses_module()


def test_seed_momentum_hypothesis_upserts_first_candidate() -> None:
    connection = FakeHypothesisConnection()
    repository = HypothesisRepository(connection)

    record = repository.upsert_hypothesis(momentum_12_1_hypothesis())

    assert record.id == 1
    assert record.hypothesis_key == "momentum_12_1"
    assert record.name == "Momentum 12-1"
    assert record.status == "proposed"
    assert record.signal_name == "momentum_12_1"
    assert record.universe_name == "falsifier_seed"
    assert record.horizon_days == 63
    assert "prior 12-month" in record.thesis
    assert connection.hypotheses["momentum_12_1"]["metadata"] == {
        "seed_source": "silver_phase1_falsifier",
        "strategy": "momentum_12_1",
    }


def test_record_backtest_evaluation_links_hypothesis_to_replayable_run() -> None:
    connection = FakeHypothesisConnection()
    repository = HypothesisRepository(connection)
    repository.upsert_hypothesis(momentum_12_1_hypothesis())
    connection.backtest_runs[3] = _backtest_row(
        id=3,
        model_run_id=7,
        backtest_run_key="falsifier-backtest-momentum_12_1-h63-abc123",
        status="succeeded",
        label_scramble_pass=True,
        metrics={"mean_strategy_net": 0.018},
    )

    evaluation = repository.record_backtest_evaluation(
        hypothesis_key="momentum_12_1",
        backtest_run_id=3,
        notes="manual replay proof matched",
    )

    assert evaluation.id == 1
    assert evaluation.hypothesis_id == 1
    assert evaluation.model_run_id == 7
    assert evaluation.backtest_run_id == 3
    assert evaluation.evaluation_status == "promising"
    assert evaluation.failure_reason is None
    assert evaluation.notes == "manual replay proof matched"
    assert evaluation.summary_metrics == {
        "backtest_status": "succeeded",
        "label_scramble_pass": True,
        "metrics": {"mean_strategy_net": 0.018},
    }
    assert connection.hypotheses["momentum_12_1"]["status"] == "promising"


def test_record_backtest_evaluation_marks_failed_scramble_as_rejected() -> None:
    connection = FakeHypothesisConnection()
    repository = HypothesisRepository(connection)
    repository.upsert_hypothesis(momentum_12_1_hypothesis())
    connection.backtest_runs[4] = _backtest_row(
        id=4,
        model_run_id=8,
        backtest_run_key="falsifier-backtest-momentum_12_1-h63-def456",
        status="succeeded",
        label_scramble_pass=False,
        metrics={"mean_strategy_net": 0.002},
    )

    evaluation = repository.record_backtest_evaluation(
        hypothesis_key="momentum_12_1",
        backtest_run_id=4,
    )

    assert evaluation.evaluation_status == "rejected"
    assert evaluation.failure_reason == "label_scramble_failed"
    assert connection.hypotheses["momentum_12_1"]["status"] == "rejected"


def test_record_latest_falsifier_evaluation_uses_latest_matching_backtest() -> None:
    connection = FakeHypothesisConnection()
    repository = HypothesisRepository(connection)
    repository.upsert_hypothesis(momentum_12_1_hypothesis())
    connection.backtest_runs[2] = _backtest_row(
        id=2,
        model_run_id=5,
        backtest_run_key="older",
        status="succeeded",
        label_scramble_pass=True,
    )
    connection.backtest_runs[3] = _backtest_row(
        id=3,
        model_run_id=7,
        backtest_run_key="newer",
        status="succeeded",
        label_scramble_pass=True,
    )

    evaluation = repository.record_latest_falsifier_evaluation(
        hypothesis_key="momentum_12_1",
        strategy="momentum_12_1",
        universe_name="falsifier_seed",
        horizon_days=63,
    )

    assert evaluation.backtest_run_id == 3
    assert evaluation.evaluation_status == "promising"


def test_list_hypotheses_includes_latest_evaluation() -> None:
    connection = FakeHypothesisConnection()
    repository = HypothesisRepository(connection)
    repository.upsert_hypothesis(momentum_12_1_hypothesis())
    connection.backtest_runs[3] = _backtest_row(id=3, model_run_id=7)
    repository.record_backtest_evaluation(
        hypothesis_key="momentum_12_1",
        backtest_run_id=3,
    )

    summaries = repository.list_hypotheses()

    assert len(summaries) == 1
    assert summaries[0].hypothesis_key == "momentum_12_1"
    assert summaries[0].latest_evaluation_status == "promising"
    assert summaries[0].latest_backtest_run_id == 3
    assert summaries[0].latest_backtest_run_key == "backtest-run-3"


def test_record_backtest_evaluation_rejects_unknown_hypothesis() -> None:
    repository = HypothesisRepository(FakeHypothesisConnection())

    with pytest.raises(
        HypothesisRegistryError,
        match="hypothesis missing was not found",
    ):
        repository.record_backtest_evaluation(
            hypothesis_key="missing",
            backtest_run_id=3,
        )


def test_seed_momentum_hypothesis_preserves_existing_lifecycle_status() -> None:
    connection = FakeHypothesisConnection()
    repository = HypothesisRepository(connection)
    repository.upsert_hypothesis(momentum_12_1_hypothesis())
    connection.backtest_runs[4] = _backtest_row(
        id=4,
        model_run_id=8,
        status="succeeded",
        label_scramble_pass=False,
    )
    repository.record_backtest_evaluation(
        hypothesis_key="momentum_12_1",
        backtest_run_id=4,
    )

    record = repository.upsert_hypothesis(momentum_12_1_hypothesis())

    assert record.status == "rejected"


def test_render_hypothesis_summaries_shows_latest_backtest_identity() -> None:
    output = manage_hypotheses.render_hypothesis_summaries(
        (
            HypothesisSummary(
                id=1,
                hypothesis_key="momentum_12_1",
                name="Momentum 12-1",
                status="proposed",
                signal_name="momentum_12_1",
                universe_name="falsifier_seed",
                horizon_days=63,
                target_kind="raw_return",
                latest_evaluation_status="promising",
                latest_failure_reason=None,
                latest_backtest_run_id=3,
                latest_backtest_run_key=(
                    "falsifier-backtest-momentum_12_1-h63-5302eb02a2b649bf"
                ),
            ),
        )
    )

    assert (
        "hypothesis_key | status | latest_eval | backtest_run_id | backtest_run_key"
    ) in output
    assert (
        "momentum_12_1 | proposed | promising | 3 | "
        "falsifier-backtest-momentum_12_1-h63-5302eb02a2b649bf"
    ) in output


def _backtest_row(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "id": 3,
        "backtest_run_key": "backtest-run-3",
        "model_run_id": 7,
        "status": "succeeded",
        "label_scramble_pass": True,
        "metrics": {"mean_strategy_net": 0.018},
        "parameters": {
            "strategy": "momentum_12_1",
            "universe": "falsifier_seed",
        },
        "universe_name": "falsifier_seed",
        "horizon_days": 63,
    }
    values.update(overrides)
    return values


class FakeHypothesisConnection:
    def __init__(self) -> None:
        self.hypotheses: dict[str, dict[str, Any]] = {}
        self.evaluations: dict[tuple[int, int], dict[str, Any]] = {}
        self.backtest_runs: dict[int, dict[str, Any]] = {}
        self.executed: list[tuple[str, dict[str, Any]]] = []
        self._next_hypothesis_id = 1
        self._next_evaluation_id = 1

    def cursor(self) -> FakeHypothesisCursor:
        return FakeHypothesisCursor(self)

    def next_hypothesis_id(self) -> int:
        value = self._next_hypothesis_id
        self._next_hypothesis_id += 1
        return value

    def next_evaluation_id(self) -> int:
        value = self._next_evaluation_id
        self._next_evaluation_id += 1
        return value


class FakeHypothesisCursor:
    def __init__(self, connection: FakeHypothesisConnection) -> None:
        self.connection = connection
        self._one: dict[str, Any] | None = None
        self._all: list[dict[str, Any]] = []

    def __enter__(self) -> FakeHypothesisCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: dict[str, Any]) -> None:
        self.connection.executed.append((sql, dict(params)))
        normalized_sql = sql.lstrip()
        if normalized_sql.startswith("INSERT INTO silver.hypotheses"):
            self._upsert_hypothesis(params)
            return
        if (
            normalized_sql.startswith("SELECT")
            and "FROM silver.hypotheses" in normalized_sql
            and "WHERE hypothesis_key" in normalized_sql
        ):
            self._one = self.connection.hypotheses.get(params["hypothesis_key"])
            return
        if (
            normalized_sql.startswith("SELECT")
            and "FROM silver.backtest_runs AS br" in normalized_sql
        ):
            self._one = self.connection.backtest_runs.get(params["backtest_run_id"])
            return
        if (
            normalized_sql.startswith("SELECT")
            and "FROM silver.backtest_runs" in normalized_sql
            and "ORDER BY id DESC" in normalized_sql
        ):
            self._one = self._latest_backtest(params)
            return
        if normalized_sql.startswith("INSERT INTO silver.hypothesis_evaluations"):
            self._upsert_evaluation(params)
            return
        if normalized_sql.startswith("UPDATE silver.hypotheses"):
            self._update_hypothesis_status(params)
            return
        if (
            normalized_sql.startswith("SELECT")
            and "FROM silver.hypotheses AS h" in normalized_sql
        ):
            self._all = self._summaries()
            return
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self) -> dict[str, Any] | None:
        return self._one

    def fetchall(self) -> list[dict[str, Any]]:
        return self._all

    def _upsert_hypothesis(self, params: dict[str, Any]) -> None:
        key = params["hypothesis_key"]
        existing = self.connection.hypotheses.get(key)
        hypothesis_id = (
            self.connection.next_hypothesis_id()
            if existing is None
            else existing["id"]
        )
        row = {
            "id": hypothesis_id,
            "hypothesis_key": key,
            "name": params["name"],
            "thesis": params["thesis"],
            "signal_name": params["signal_name"],
            "mechanism": params["mechanism"],
            "universe_name": params["universe_name"],
            "horizon_days": params["horizon_days"],
            "target_kind": params["target_kind"],
            "status": params["status"] if existing is None else existing["status"],
            "metadata": json.loads(params["metadata"]),
        }
        self.connection.hypotheses[key] = row
        self._one = row

    def _latest_backtest(self, params: dict[str, Any]) -> dict[str, Any] | None:
        candidates = [
            row
            for row in self.connection.backtest_runs.values()
            if row["parameters"].get("strategy") == params["strategy"]
            and row["universe_name"] == params["universe_name"]
            and row["horizon_days"] == params["horizon_days"]
            and row["status"] == "succeeded"
        ]
        if not candidates:
            return None
        return {"id": max(candidates, key=lambda row: row["id"])["id"]}

    def _upsert_evaluation(self, params: dict[str, Any]) -> None:
        key = (params["hypothesis_id"], params["backtest_run_id"])
        existing = self.connection.evaluations.get(key)
        evaluation_id = (
            self.connection.next_evaluation_id()
            if existing is None
            else existing["id"]
        )
        row = {
            "id": evaluation_id,
            "hypothesis_id": params["hypothesis_id"],
            "model_run_id": params["model_run_id"],
            "backtest_run_id": params["backtest_run_id"],
            "evaluation_status": params["evaluation_status"],
            "failure_reason": params["failure_reason"],
            "notes": params["notes"],
            "summary_metrics": json.loads(params["summary_metrics"]),
        }
        self.connection.evaluations[key] = row
        self._one = row

    def _update_hypothesis_status(self, params: dict[str, Any]) -> None:
        for row in self.connection.hypotheses.values():
            if row["id"] == params["hypothesis_id"]:
                row["status"] = params["status"]
                self._one = None
                return
        raise AssertionError(f"unknown hypothesis id: {params['hypothesis_id']}")

    def _summaries(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for hypothesis in self.connection.hypotheses.values():
            evaluations = [
                row
                for row in self.connection.evaluations.values()
                if row["hypothesis_id"] == hypothesis["id"]
            ]
            latest = max(evaluations, key=lambda row: row["id"], default=None)
            backtest = (
                self.connection.backtest_runs.get(latest["backtest_run_id"])
                if latest is not None
                else None
            )
            rows.append(
                {
                    **hypothesis,
                    "latest_evaluation_status": (
                        latest["evaluation_status"] if latest else None
                    ),
                    "latest_failure_reason": latest["failure_reason"] if latest else None,
                    "latest_backtest_run_id": (
                        latest["backtest_run_id"] if latest else None
                    ),
                    "latest_backtest_run_key": (
                        backtest["backtest_run_key"] if backtest else None
                    ),
                }
            )
        return sorted(rows, key=lambda row: row["hypothesis_key"])
