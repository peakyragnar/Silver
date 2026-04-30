from __future__ import annotations

import json
from dataclasses import fields, replace
from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from silver.analytics import (
    BacktestMetadataError,
    BacktestMetadataRepository,
    compare_backtest_replay_snapshots,
    BacktestRunCreate,
    BacktestRunFinish,
    AnalyticsRunRepository,
    ModelRunCreate,
    ModelRunFinish,
)

REPLAY_SNAPSHOT_FIELDS = {
    "model_run_id",
    "model_run_key",
    "model_status",
    "model_code_git_sha",
    "model_feature_set_hash",
    "model_feature_snapshot_ref",
    "model_training_start_date",
    "model_training_end_date",
    "model_test_start_date",
    "model_test_end_date",
    "model_horizon_days",
    "model_target_kind",
    "model_random_seed",
    "model_cost_assumptions",
    "model_metrics",
    "model_parameters",
    "model_available_at_policy_versions",
    "model_input_fingerprints",
    "backtest_run_id",
    "backtest_run_key",
    "backtest_status",
    "backtest_model_run_id",
    "backtest_universe_name",
    "backtest_horizon_days",
    "backtest_target_kind",
    "backtest_cost_assumptions",
    "backtest_metrics",
    "backtest_metrics_by_regime",
    "backtest_baseline_metrics",
    "backtest_label_scramble_metrics",
    "backtest_label_scramble_pass",
    "backtest_parameters",
    "backtest_multiple_comparisons_correction",
}

INVOCATION_ONLY_FIELDS = {
    "invocation_id",
    "process_id",
    "host_name",
    "user_name",
    "output_path",
    "report_path",
    "started_at",
    "finished_at",
    "created_at",
}


def test_model_run_lifecycle_supports_terminal_statuses() -> None:
    connection = FakeMetadataConnection()
    repository = BacktestMetadataRepository(connection)

    for index, status in enumerate(
        ("succeeded", "failed", "insufficient_data"),
        start=1,
    ):
        run = _model_run_create(
            model_run_key=f"model-run-{status}",
            name=f"Model {index}",
        )
        created = repository.create_model_run(run)
        metrics = {"sharpe_net": 0.42} if status == "succeeded" else {}

        finished = repository.finish_model_run(
            created.id,
            ModelRunFinish(status=status, metrics=metrics),
        )

        assert created.model_run_key == run.model_run_key
        assert created.status == "running"
        assert finished.id == created.id
        assert finished.status == status
        assert connection.model_runs[run.model_run_key]["metrics"] == metrics

    insert_sql, _insert_params = connection.executed[0]
    update_sql, _update_params = connection.executed[1]
    assert insert_sql.startswith("INSERT INTO silver.model_runs")
    assert "ON CONFLICT (model_run_key) DO NOTHING" in insert_sql
    assert update_sql.startswith("UPDATE silver.model_runs")
    assert "AND status = 'running'" in update_sql


def test_backtest_run_lifecycle_writes_linked_metadata() -> None:
    connection = FakeMetadataConnection()
    repository = BacktestMetadataRepository(connection)

    model = repository.create_model_run(_model_run_create())
    backtest = repository.create_backtest_run(
        _backtest_run_create(model_run_id=model.id),
    )
    finished = repository.finish_backtest_run(backtest.id, _backtest_run_finish())

    assert backtest.backtest_run_key == "backtest-run-1"
    assert backtest.status == "running"
    assert finished.id == backtest.id
    assert finished.status == "succeeded"

    stored = connection.backtest_runs["backtest-run-1"]
    assert stored["model_run_id"] == model.id
    assert stored["metrics"] == {"sharpe_net": 0.71, "turnover": 0.18}
    assert stored["baseline_metrics"] == {
        "equal_weight": {"sharpe_net": 0.08},
        "momentum_12_1": {"sharpe_net": 0.35},
    }
    assert stored["label_scramble_pass"] is True
    assert stored["cost_assumptions"] == {
        "borrow_bps_annual": 25,
        "half_spread_bps": 5,
    }

    insert_sql, insert_params = connection.executed[1]
    update_sql, update_params = connection.executed[2]
    assert insert_sql.startswith("INSERT INTO silver.backtest_runs")
    assert insert_params["model_run_id"] == model.id
    assert update_sql.startswith("UPDATE silver.backtest_runs")
    assert "baseline_metrics = %(baseline_metrics)s::jsonb" in update_sql
    assert "label_scramble_pass = %(label_scramble_pass)s" in update_sql
    assert update_params["multiple_comparisons_correction"] == "bh"


def test_traceability_snapshot_resolves_backtest_run_to_model_run_metadata() -> None:
    connection = FakeMetadataConnection()
    repository = BacktestMetadataRepository(connection)

    model = repository.create_model_run(_model_run_create())
    backtest = repository.create_backtest_run(
        _backtest_run_create(model_run_id=model.id),
    )
    repository.finish_model_run(
        model.id,
        ModelRunFinish(status="succeeded", metrics={"split_count": 3}),
    )
    repository.finish_backtest_run(backtest.id, _backtest_run_finish())

    snapshot = repository.load_backtest_traceability_snapshot(backtest.id)

    assert snapshot.model_run_id == model.id
    assert snapshot.model_run_key == "model-run-1"
    assert snapshot.model_status == "succeeded"
    assert snapshot.model_code_git_sha == "abcdef0"
    assert snapshot.model_feature_set_hash == "a" * 64
    assert snapshot.model_feature_snapshot_ref == "feature-snapshot:v1"
    assert snapshot.model_training_start_date == date(2020, 1, 2)
    assert snapshot.model_training_end_date == date(2021, 12, 31)
    assert snapshot.model_test_start_date == date(2022, 1, 3)
    assert snapshot.model_test_end_date == date(2024, 12, 31)
    assert snapshot.model_horizon_days == 63
    assert snapshot.model_parameters == {"strategy": "momentum_12_1"}
    assert snapshot.model_available_at_policy_versions == {"daily_price": 1}
    assert snapshot.backtest_run_id == backtest.id
    assert snapshot.backtest_model_run_id == model.id
    assert snapshot.backtest_universe_name == "falsifier_seed"
    assert snapshot.backtest_horizon_days == 63
    assert snapshot.backtest_parameters == {"rebalance": "monthly"}
    assert snapshot.backtest_metrics == {"sharpe_net": 0.71, "turnover": 0.18}
    assert snapshot.backtest_metrics_by_regime["pre_2019"]["sharpe_net"] == 0.52
    assert snapshot.backtest_baseline_metrics["equal_weight"]["sharpe_net"] == 0.08
    assert snapshot.backtest_label_scramble_metrics == {
        "scrambled_sharpe_net": 0.01,
    }
    assert snapshot.backtest_label_scramble_pass is True

    sql, params = connection.executed[-1]
    assert sql.startswith("SELECT\n    mr.id AS model_run_id")
    assert "JOIN silver.model_runs mr ON mr.id = br.model_run_id" in sql
    assert "br.metrics_by_regime AS backtest_metrics_by_regime" in sql
    assert "br.label_scramble_metrics AS backtest_label_scramble_metrics" in sql
    assert "br.label_scramble_pass AS backtest_label_scramble_pass" in sql
    assert params == {"backtest_run_id": backtest.id}


def test_traceability_snapshot_is_replay_contract_from_backtest_run_id() -> None:
    connection = FakeMetadataConnection()
    repository = BacktestMetadataRepository(connection)

    model = repository.create_model_run(_model_run_create())
    backtest = repository.create_backtest_run(_backtest_run_create(model_run_id=model.id))
    repository.finish_model_run(
        model.id,
        ModelRunFinish(status="succeeded", metrics={"split_count": 3}),
    )
    repository.finish_backtest_run(backtest.id, _backtest_run_finish())

    snapshot = repository.load_backtest_traceability_snapshot(backtest.id)
    snapshot_fields = {field.name for field in fields(snapshot)}

    assert REPLAY_SNAPSHOT_FIELDS <= snapshot_fields
    assert INVOCATION_ONLY_FIELDS.isdisjoint(snapshot_fields)
    assert snapshot.backtest_run_id == backtest.id
    assert snapshot.backtest_model_run_id == model.id
    assert snapshot.model_run_id == model.id
    assert snapshot.model_code_git_sha == "abcdef0"
    assert snapshot.model_feature_set_hash == "a" * 64
    assert snapshot.model_feature_snapshot_ref == "feature-snapshot:v1"
    assert snapshot.model_input_fingerprints == {"universe": "falsifier_seed"}
    assert snapshot.model_available_at_policy_versions == {"daily_price": 1}
    assert snapshot.model_random_seed == 7
    assert snapshot.backtest_universe_name == "falsifier_seed"
    assert snapshot.backtest_multiple_comparisons_correction == "bh"
    assert snapshot.backtest_label_scramble_pass is True


def test_replay_snapshot_resolves_accepted_backtest_claim_by_id_or_key() -> None:
    connection = FakeMetadataConnection()
    repository = BacktestMetadataRepository(connection)

    model = repository.create_model_run(_model_run_create())
    backtest = repository.create_backtest_run(_backtest_run_create(model_run_id=model.id))
    repository.finish_model_run(
        model.id,
        ModelRunFinish(status="succeeded", metrics={"split_count": 3}),
    )
    repository.finish_backtest_run(backtest.id, _backtest_run_finish())

    by_id = repository.load_backtest_replay_snapshot(backtest_run_id=backtest.id)
    by_key = repository.load_backtest_replay_snapshot(
        backtest_run_key=backtest.backtest_run_key,
    )

    assert by_id == by_key
    assert by_key.backtest_run_key == "backtest-run-1"
    assert by_key.model_run_key == "model-run-1"
    assert connection.executed[-2][1] == {"backtest_run_id": backtest.id}
    assert connection.executed[-1][1] == {"backtest_run_key": backtest.backtest_run_key}


def test_replay_snapshot_requires_exactly_one_backtest_identity() -> None:
    repository = BacktestMetadataRepository(FakeMetadataConnection())

    with pytest.raises(BacktestMetadataError, match="exactly one backtest identity"):
        repository.load_backtest_replay_snapshot()

    with pytest.raises(BacktestMetadataError, match="exactly one backtest identity"):
        repository.load_backtest_replay_snapshot(
            backtest_run_id=1,
            backtest_run_key="backtest-run-1",
        )


def test_model_run_replay_metadata_resolves_by_id_or_key() -> None:
    connection = FakeMetadataConnection()
    repository = BacktestMetadataRepository(connection)

    model = repository.create_model_run(_model_run_create())
    repository.finish_model_run(
        model.id,
        ModelRunFinish(status="succeeded", metrics={"split_count": 3}),
    )

    by_id = repository.load_model_run_replay_metadata(model_run_id=model.id)
    by_key = repository.load_model_run_replay_metadata(model_run_key=model.model_run_key)

    assert by_id == by_key
    assert by_key.model_run_id == model.id
    assert by_key.model_run_key == "model-run-1"
    assert by_key.status == "succeeded"
    assert by_key.feature_set_hash == "a" * 64
    assert by_key.input_fingerprints == {"universe": "falsifier_seed"}


def test_backtest_replay_comparison_names_drifted_identity_field() -> None:
    connection = FakeMetadataConnection()
    repository = BacktestMetadataRepository(connection)
    model = repository.create_model_run(_model_run_create())
    backtest = repository.create_backtest_run(_backtest_run_create(model_run_id=model.id))
    repository.finish_model_run(
        model.id,
        ModelRunFinish(status="succeeded", metrics={"split_count": 3}),
    )
    repository.finish_backtest_run(backtest.id, _backtest_run_finish())

    stored = repository.load_backtest_replay_snapshot(backtest_run_id=backtest.id)
    drifted = replace(stored, model_available_at_policy_versions={"daily_price": 2})

    comparison = compare_backtest_replay_snapshots(stored, drifted)

    assert not comparison.matches
    assert comparison.mismatches == (
        "model_runs.available_at_policy_versions expected {\"daily_price\":1} "
        "got {\"daily_price\":2}",
    )


def test_traceability_snapshot_rejects_missing_backtest_run_id() -> None:
    connection = FakeMetadataConnection()
    repository = BacktestMetadataRepository(connection)

    with pytest.raises(
        BacktestMetadataError,
        match="traceability metadata was not found",
    ):
        repository.load_backtest_traceability_snapshot(999)

    sql, params = connection.executed[-1]
    assert sql.startswith("SELECT\n    mr.id AS model_run_id")
    assert params == {"backtest_run_id": 999}


def test_analytics_run_lifecycle_stores_invocation_metadata_with_run_keys() -> None:
    connection = FakeMetadataConnection()
    repository = AnalyticsRunRepository(connection)

    run = repository.create_run(
        run_kind="falsifier_report_invocation",
        code_git_sha="abcdef0",
        available_at_policy_versions={"daily_price": 1},
        parameters={
            "invocation_id": "12345678-1234-5678-1234-567812345678",
            "model_run_key": "model-run-1",
            "backtest_run_key": "backtest-run-1",
            "output_path": "reports/falsifier/week_1_momentum.md",
            "process_id": 4321,
        },
        input_fingerprints={"joined_feature_label_rows_sha256": "f" * 64},
        random_seed=0,
    )
    finished = repository.finish_run(run.id, status="succeeded")

    assert run.run_kind == "falsifier_report_invocation"
    assert run.status == "running"
    assert finished.id == run.id
    assert finished.status == "succeeded"
    stored = connection.analytics_runs[run.id]
    assert stored["status"] == "succeeded"
    assert stored["parameters"]["invocation_id"] == (
        "12345678-1234-5678-1234-567812345678"
    )
    assert stored["parameters"]["model_run_key"] == "model-run-1"
    assert stored["parameters"]["backtest_run_key"] == "backtest-run-1"
    assert stored["input_fingerprints"] == {
        "joined_feature_label_rows_sha256": "f" * 64,
    }


def test_create_model_run_is_idempotent_for_same_key() -> None:
    connection = FakeMetadataConnection()
    repository = BacktestMetadataRepository(connection)
    run = _model_run_create()

    first = repository.create_model_run(run)
    second = repository.create_model_run(run)

    assert first == second
    assert len(connection.model_runs) == 1
    assert [sql for sql, _params in connection.executed].count(
        connection.insert_model_sql,
    ) == 2


def test_create_model_run_rejects_same_key_with_different_metadata() -> None:
    connection = FakeMetadataConnection()
    repository = BacktestMetadataRepository(connection)
    run = _model_run_create()
    repository.create_model_run(run)

    with pytest.raises(BacktestMetadataError, match="different metadata"):
        repository.create_model_run(replace(run, random_seed=99))


@pytest.mark.parametrize(
    "override",
    (
        pytest.param({"code_git_sha": "badcafe"}, id="code_git_sha"),
        pytest.param({"feature_set_hash": "b" * 64}, id="feature_set_hash"),
        pytest.param(
            {"feature_snapshot_ref": "feature-snapshot:v2"},
            id="feature_snapshot_ref",
        ),
        pytest.param(
            {"cost_assumptions": {"half_spread_bps": 10}},
            id="cost_assumptions",
        ),
        pytest.param(
            {"available_at_policy_versions": {"daily_price": 2}},
            id="available_at_policy_versions",
        ),
        pytest.param(
            {"input_fingerprints": {"universe": "expanded_seed"}},
            id="input_fingerprints",
        ),
    ),
)
def test_model_run_replay_identity_rejects_same_key_with_changed_stable_input(
    override: dict[str, Any],
) -> None:
    connection = FakeMetadataConnection()
    repository = BacktestMetadataRepository(connection)
    run = _model_run_create()
    repository.create_model_run(run)

    with pytest.raises(BacktestMetadataError, match="different metadata"):
        repository.create_model_run(replace(run, **override))


def test_backtest_run_replay_identity_rejects_same_key_metadata_drift() -> None:
    connection = FakeMetadataConnection()
    repository = BacktestMetadataRepository(connection)
    model = repository.create_model_run(_model_run_create())
    run = _backtest_run_create(model_run_id=model.id)
    repository.create_backtest_run(run)

    with pytest.raises(BacktestMetadataError, match="different metadata"):
        repository.create_backtest_run(
            replace(run, universe_name="expanded_seed"),
        )


def test_invalid_inputs_are_rejected_before_sql_execution() -> None:
    cases = [
        lambda repo: repo.create_model_run(
            replace(_model_run_create(), horizon_days=10),
        ),
        lambda repo: repo.create_model_run(
            replace(_model_run_create(), parameters=["not", "object"]),
        ),
        lambda repo: repo.create_model_run(
            replace(_model_run_create(), cost_assumptions={}),
        ),
        lambda repo: repo.create_model_run(
            replace(_model_run_create(), available_at_policy_versions={}),
        ),
        lambda repo: repo.create_model_run(
            replace(
                _model_run_create(),
                feature_snapshot_ref=None,
                input_fingerprints={},
            ),
        ),
        lambda repo: repo.finish_model_run(
            1,
            ModelRunFinish(status="running"),
        ),
        lambda repo: repo.create_backtest_run(
            replace(_backtest_run_create(), model_run_id=0),
        ),
        lambda repo: repo.create_backtest_run(
            replace(_backtest_run_create(), cost_assumptions={}),
        ),
        lambda repo: repo.finish_backtest_run(
            1,
            BacktestRunFinish(
                status="succeeded",
                cost_assumptions={"half_spread_bps": 5},
                metrics={"sharpe_net": 0.5},
                baseline_metrics={},
                label_scramble_pass=True,
            ),
        ),
        lambda repo: repo.finish_backtest_run(
            1,
            BacktestRunFinish(
                status="succeeded",
                cost_assumptions={"half_spread_bps": 5},
                metrics={"sharpe_net": 0.5},
                metrics_by_regime={},
                baseline_metrics={"equal_weight": {"sharpe_net": 0.1}},
                label_scramble_metrics={"scrambled_sharpe_net": 0.01},
                label_scramble_pass=True,
            ),
        ),
        lambda repo: repo.finish_backtest_run(
            1,
            BacktestRunFinish(
                status="succeeded",
                cost_assumptions={"half_spread_bps": 5},
                metrics={"sharpe_net": 0.5},
                metrics_by_regime={"pre_2019": {"sharpe_net": 0.2}},
                baseline_metrics={"equal_weight": {"sharpe_net": 0.1}},
                label_scramble_metrics={},
                label_scramble_pass=True,
            ),
        ),
        lambda repo: repo.finish_backtest_run(
            1,
            BacktestRunFinish(
                status="insufficient_data",
                label_scramble_pass=None,
            ),
        ),
    ]

    for call in cases:
        connection = FakeMetadataConnection()
        repository = BacktestMetadataRepository(connection)

        with pytest.raises(BacktestMetadataError):
            call(repository)

        assert connection.executed == []


def test_finish_missing_rows_raises_clear_errors() -> None:
    repository = BacktestMetadataRepository(FakeMetadataConnection())

    with pytest.raises(BacktestMetadataError, match="model run 99 was not found"):
        repository.finish_model_run(
            99,
            ModelRunFinish(status="failed"),
        )

    with pytest.raises(BacktestMetadataError, match="backtest run 99 was not found"):
        repository.finish_backtest_run(
            99,
            BacktestRunFinish(status="failed"),
        )


def test_stable_json_serialization_for_create_and_finish_payloads() -> None:
    connection = FakeMetadataConnection()
    repository = BacktestMetadataRepository(connection)

    model = repository.create_model_run(
        _model_run_create(
            cost_assumptions={"z": 2, "a": Decimal("1.5")},
            parameters={"z": [3, {"b": 2, "a": 1}], "a": True},
            available_at_policy_versions={"prices": 1, "filings": 2},
            input_fingerprints={"universe": {"sha256": "f" * 64}},
        ),
    )
    backtest = repository.create_backtest_run(
        _backtest_run_create(
            model_run_id=model.id,
            parameters={"z": 1, "a": 2},
        ),
    )
    repository.finish_backtest_run(
        backtest.id,
        _backtest_run_finish(
            metrics={"z": 1, "a": 2},
            label_scramble_metrics={"p_value": 0.42, "n": 100},
        ),
    )

    _model_sql, model_params = connection.executed[0]
    _backtest_insert_sql, backtest_insert_params = connection.executed[1]
    _backtest_update_sql, backtest_update_params = connection.executed[2]

    assert model_params["cost_assumptions"] == '{"a":"1.5","z":2}'
    assert model_params["parameters"] == '{"a":true,"z":[3,{"a":1,"b":2}]}'
    assert model_params["available_at_policy_versions"] == '{"filings":2,"prices":1}'
    assert backtest_insert_params["parameters"] == '{"a":2,"z":1}'
    assert backtest_update_params["metrics"] == '{"a":2,"z":1}'
    assert backtest_update_params["label_scramble_metrics"] == (
        '{"n":100,"p_value":0.42}'
    )


def _model_run_create(**overrides: Any) -> ModelRunCreate:
    values = {
        "model_run_key": "model-run-1",
        "name": "Momentum 12-1 model",
        "code_git_sha": "abcdef0",
        "feature_set_hash": "a" * 64,
        "feature_snapshot_ref": "feature-snapshot:v1",
        "training_start_date": date(2020, 1, 2),
        "training_end_date": date(2021, 12, 31),
        "test_start_date": date(2022, 1, 3),
        "test_end_date": date(2024, 12, 31),
        "horizon_days": 63,
        "target_kind": "excess_return_market",
        "random_seed": 7,
        "cost_assumptions": {"half_spread_bps": 5},
        "parameters": {"strategy": "momentum_12_1"},
        "available_at_policy_versions": {"daily_price": 1},
        "input_fingerprints": {"universe": "falsifier_seed"},
    }
    values.update(overrides)
    return ModelRunCreate(**values)


def _backtest_run_create(**overrides: Any) -> BacktestRunCreate:
    values = {
        "backtest_run_key": "backtest-run-1",
        "model_run_id": 1,
        "name": "Momentum 12-1 falsifier",
        "universe_name": "falsifier_seed",
        "horizon_days": 63,
        "target_kind": "excess_return_market",
        "cost_assumptions": {"half_spread_bps": 5},
        "parameters": {"rebalance": "monthly"},
        "multiple_comparisons_correction": "bh",
    }
    values.update(overrides)
    return BacktestRunCreate(**values)


def _backtest_run_finish(**overrides: Any) -> BacktestRunFinish:
    values = {
        "status": "succeeded",
        "cost_assumptions": {
            "half_spread_bps": 5,
            "borrow_bps_annual": 25,
        },
        "metrics": {
            "sharpe_net": 0.71,
            "turnover": 0.18,
        },
        "metrics_by_regime": {
            "pre_2019": {"sharpe_net": 0.52},
            "2020_dislocation": {"sharpe_net": 0.12},
        },
        "baseline_metrics": {
            "equal_weight": {"sharpe_net": 0.08},
            "momentum_12_1": {"sharpe_net": 0.35},
        },
        "label_scramble_metrics": {"scrambled_sharpe_net": 0.01},
        "label_scramble_pass": True,
        "multiple_comparisons_correction": "bh",
    }
    values.update(overrides)
    return BacktestRunFinish(**values)


class FakeMetadataConnection:
    insert_model_sql = ""

    def __init__(self) -> None:
        self.analytics_runs: dict[int, dict[str, Any]] = {}
        self.model_runs: dict[str, dict[str, Any]] = {}
        self.backtest_runs: dict[str, dict[str, Any]] = {}
        self.executed: list[tuple[str, dict[str, Any]]] = []
        self._next_run_id = 1
        self._next_model_id = 1
        self._next_backtest_id = 101

    def cursor(self) -> FakeMetadataCursor:
        return FakeMetadataCursor(self)

    def next_run_id(self) -> int:
        value = self._next_run_id
        self._next_run_id += 1
        return value

    def next_model_id(self) -> int:
        value = self._next_model_id
        self._next_model_id += 1
        return value

    def next_backtest_id(self) -> int:
        value = self._next_backtest_id
        self._next_backtest_id += 1
        return value


class FakeMetadataCursor:
    def __init__(self, connection: FakeMetadataConnection) -> None:
        self.connection = connection
        self._one: dict[str, Any] | None = None

    def __enter__(self) -> FakeMetadataCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: dict[str, Any]) -> None:
        self.connection.executed.append((sql, dict(params)))
        if sql.startswith("INSERT INTO silver.analytics_runs"):
            self._insert_analytics_run(params)
            return
        if sql.startswith("UPDATE silver.analytics_runs"):
            self._finish_analytics_run(params)
            return
        if sql.startswith("INSERT INTO silver.model_runs"):
            self.connection.insert_model_sql = sql
            self._insert_model_run(params)
            return
        if sql.startswith("SELECT\n    id,\n    model_run_key"):
            self._one = self.connection.model_runs.get(params["model_run_key"])
            return
        if sql.startswith("SELECT\n    id AS model_run_id"):
            self._one = self._model_run_replay_metadata(params)
            return
        if sql.startswith("UPDATE silver.model_runs"):
            self._finish_model_run(params)
            return
        if sql.startswith("INSERT INTO silver.backtest_runs"):
            self._insert_backtest_run(params)
            return
        if sql.startswith("SELECT\n    id,\n    backtest_run_key"):
            self._one = self.connection.backtest_runs.get(params["backtest_run_key"])
            return
        if sql.startswith("UPDATE silver.backtest_runs"):
            self._finish_backtest_run(params)
            return
        if sql.startswith("SELECT\n    mr.id AS model_run_id"):
            self._one = self._traceability_snapshot(params)
            return
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self) -> dict[str, Any] | None:
        return self._one

    def _insert_analytics_run(self, params: dict[str, Any]) -> None:
        run_id = self.connection.next_run_id()
        row = {
            "id": run_id,
            "run_kind": params["run_kind"],
            "status": "running",
            "code_git_sha": params["code_git_sha"],
            "available_at_policy_versions": json.loads(
                params["available_at_policy_versions"],
            ),
            "parameters": json.loads(params["parameters"]),
            "input_fingerprints": json.loads(params["input_fingerprints"]),
            "random_seed": params["random_seed"],
        }
        self.connection.analytics_runs[run_id] = row
        self._one = row

    def _finish_analytics_run(self, params: dict[str, Any]) -> None:
        row = self.connection.analytics_runs.get(params["run_id"])
        if row is None:
            self._one = None
            return
        row["status"] = params["status"]
        self._one = row

    def _insert_model_run(self, params: dict[str, Any]) -> None:
        key = params["model_run_key"]
        if key in self.connection.model_runs:
            self._one = None
            return
        row = {
            "id": self.connection.next_model_id(),
            "model_run_key": key,
            "status": "running",
            "name": params["name"],
            "code_git_sha": params["code_git_sha"],
            "feature_set_hash": params["feature_set_hash"],
            "feature_snapshot_ref": params["feature_snapshot_ref"],
            "training_start_date": params["training_start_date"],
            "training_end_date": params["training_end_date"],
            "test_start_date": params["test_start_date"],
            "test_end_date": params["test_end_date"],
            "horizon_days": params["horizon_days"],
            "target_kind": params["target_kind"],
            "random_seed": params["random_seed"],
            "cost_assumptions": json.loads(params["cost_assumptions"]),
            "parameters": json.loads(params["parameters"]),
            "available_at_policy_versions": json.loads(
                params["available_at_policy_versions"],
            ),
            "input_fingerprints": json.loads(params["input_fingerprints"]),
            "metrics": {},
        }
        self.connection.model_runs[key] = row
        self._one = row

    def _finish_model_run(self, params: dict[str, Any]) -> None:
        for row in self.connection.model_runs.values():
            if row["id"] == params["run_id"] and row["status"] == "running":
                row["status"] = params["status"]
                row["metrics"] = json.loads(params["metrics"])
                self._one = row
                return
        self._one = None

    def _insert_backtest_run(self, params: dict[str, Any]) -> None:
        key = params["backtest_run_key"]
        if key in self.connection.backtest_runs:
            self._one = None
            return
        row = {
            "id": self.connection.next_backtest_id(),
            "backtest_run_key": key,
            "status": "running",
            "model_run_id": params["model_run_id"],
            "name": params["name"],
            "universe_name": params["universe_name"],
            "horizon_days": params["horizon_days"],
            "target_kind": params["target_kind"],
            "cost_assumptions": json.loads(params["cost_assumptions"]),
            "parameters": json.loads(params["parameters"]),
            "multiple_comparisons_correction": params[
                "multiple_comparisons_correction"
            ],
            "metrics": {},
            "metrics_by_regime": {},
            "baseline_metrics": {},
            "label_scramble_metrics": {},
            "label_scramble_pass": None,
        }
        self.connection.backtest_runs[key] = row
        self._one = row

    def _finish_backtest_run(self, params: dict[str, Any]) -> None:
        for row in self.connection.backtest_runs.values():
            if row["id"] == params["run_id"] and row["status"] == "running":
                row["status"] = params["status"]
                row["cost_assumptions"] = json.loads(params["cost_assumptions"])
                row["metrics"] = json.loads(params["metrics"])
                row["metrics_by_regime"] = json.loads(params["metrics_by_regime"])
                row["baseline_metrics"] = json.loads(params["baseline_metrics"])
                row["label_scramble_metrics"] = json.loads(
                    params["label_scramble_metrics"],
                )
                row["label_scramble_pass"] = params["label_scramble_pass"]
                row["multiple_comparisons_correction"] = params[
                    "multiple_comparisons_correction"
                ]
                self._one = row
                return
        self._one = None

    def _model_run_replay_metadata(self, params: dict[str, Any]) -> dict[str, Any] | None:
        for model in self.connection.model_runs.values():
            if params.get("model_run_id") not in {None, model["id"]}:
                continue
            if params.get("model_run_key") not in {None, model["model_run_key"]}:
                continue
            return {
                "model_run_id": model["id"],
                "model_run_key": model["model_run_key"],
                "status": model["status"],
                "code_git_sha": model["code_git_sha"],
                "feature_set_hash": model["feature_set_hash"],
                "feature_snapshot_ref": model["feature_snapshot_ref"],
                "training_start_date": model["training_start_date"],
                "training_end_date": model["training_end_date"],
                "test_start_date": model["test_start_date"],
                "test_end_date": model["test_end_date"],
                "horizon_days": model["horizon_days"],
                "target_kind": model["target_kind"],
                "random_seed": model["random_seed"],
                "cost_assumptions": model["cost_assumptions"],
                "metrics": model["metrics"],
                "parameters": model["parameters"],
                "available_at_policy_versions": model[
                    "available_at_policy_versions"
                ],
                "input_fingerprints": model["input_fingerprints"],
            }
        return None

    def _traceability_snapshot(self, params: dict[str, Any]) -> dict[str, Any] | None:
        for backtest in self.connection.backtest_runs.values():
            if params.get("backtest_run_id") not in {None, backtest["id"]}:
                continue
            if params.get("backtest_run_key") not in {None, backtest["backtest_run_key"]}:
                continue
            model = next(
                row
                for row in self.connection.model_runs.values()
                if row["id"] == backtest["model_run_id"]
            )
            return {
                "model_run_id": model["id"],
                "model_run_key": model["model_run_key"],
                "model_status": model["status"],
                "model_code_git_sha": model["code_git_sha"],
                "model_feature_set_hash": model["feature_set_hash"],
                "model_feature_snapshot_ref": model["feature_snapshot_ref"],
                "model_training_start_date": model["training_start_date"],
                "model_training_end_date": model["training_end_date"],
                "model_test_start_date": model["test_start_date"],
                "model_test_end_date": model["test_end_date"],
                "model_horizon_days": model["horizon_days"],
                "model_target_kind": model["target_kind"],
                "model_random_seed": model["random_seed"],
                "model_cost_assumptions": model["cost_assumptions"],
                "model_metrics": model["metrics"],
                "model_parameters": model["parameters"],
                "model_available_at_policy_versions": model[
                    "available_at_policy_versions"
                ],
                "model_input_fingerprints": model["input_fingerprints"],
                "backtest_run_id": backtest["id"],
                "backtest_run_key": backtest["backtest_run_key"],
                "backtest_status": backtest["status"],
                "backtest_model_run_id": backtest["model_run_id"],
                "backtest_universe_name": backtest["universe_name"],
                "backtest_horizon_days": backtest["horizon_days"],
                "backtest_target_kind": backtest["target_kind"],
                "backtest_cost_assumptions": backtest["cost_assumptions"],
                "backtest_metrics": backtest["metrics"],
                "backtest_metrics_by_regime": backtest["metrics_by_regime"],
                "backtest_baseline_metrics": backtest["baseline_metrics"],
                "backtest_label_scramble_metrics": backtest[
                    "label_scramble_metrics"
                ],
                "backtest_label_scramble_pass": backtest["label_scramble_pass"],
                "backtest_parameters": backtest["parameters"],
                "backtest_multiple_comparisons_correction": backtest[
                    "multiple_comparisons_correction"
                ],
            }
        return None
