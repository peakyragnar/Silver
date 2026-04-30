from __future__ import annotations

import importlib.util
import subprocess
import sys
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUN_FALSIFIER_SCRIPT = ROOT / "scripts" / "run_falsifier.py"
CLI_SPEC = importlib.util.spec_from_file_location("run_falsifier_cli", RUN_FALSIFIER_SCRIPT)
assert CLI_SPEC is not None
assert CLI_SPEC.loader is not None
cli = importlib.util.module_from_spec(CLI_SPEC)
sys.modules[CLI_SPEC.name] = cli
CLI_SPEC.loader.exec_module(cli)

from silver.backtest.momentum_falsifier import (  # noqa: E402
    MomentumFalsifierResult,
    MomentumHeadlineMetrics,
    MomentumWindowResult,
)


def test_check_mode_validates_without_database_url() -> None:
    result = subprocess.run(
        [sys.executable, str(RUN_FALSIFIER_SCRIPT), "--check"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "OK: falsifier CLI check passed" in result.stdout
    assert "reports/falsifier/week_1_momentum.md" in result.stdout


def test_check_mode_does_not_open_metadata_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_connect(_database_url: str) -> object:
        raise AssertionError("check mode must not connect for metadata writes")

    monkeypatch.setattr(cli, "_connect_metadata_database", fail_connect)

    cli.run_check(cli.parse_args(["--check"]))


def test_apply_mode_requires_database_url() -> None:
    result = subprocess.run(
        [sys.executable, str(RUN_FALSIFIER_SCRIPT)],
        text=True,
        capture_output=True,
        check=False,
        env={},
    )

    assert result.returncode == 1
    assert "DATABASE_URL is required unless --check is used" in result.stderr


def test_report_run_creates_and_finishes_model_and_backtest_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calendar = _calendar()
    rows = _momentum_rows(calendar, session_count=420)
    feature = _feature_definition()
    repo = FakeMetadataRepository()
    args = cli.parse_args(
        [
            "--database-url",
            "postgresql://user:pass@localhost/silver",
            "--output-path",
            str(tmp_path / "report.md"),
        ]
    )
    monkeypatch.setattr(cli, "_git_sha", lambda: "abcdef0")
    monkeypatch.setattr(cli, "run_label_scramble", _fake_label_scramble)
    monkeypatch.setattr(
        cli,
        "load_persisted_inputs",
        lambda *_args, **_kwargs: _persisted_inputs(rows=rows),
    )

    outcome = cli.run_report_with_metadata(
        args,
        client=object(),
        metadata_repository=repo,
        calendar=calendar,
    )

    assert outcome.status == "succeeded"
    assert outcome.model_run.status == "succeeded"
    assert outcome.backtest_run.status == "succeeded"
    assert len(repo.model_creates) == 1
    assert len(repo.model_finishes) == 1
    assert len(repo.backtest_creates) == 1
    assert len(repo.backtest_finishes) == 1
    assert repo.traceability_loads == [2]

    model_create = repo.model_creates[0]
    assert model_create.name == "Momentum 12-1 falsifier"
    assert model_create.code_git_sha == "abcdef0"
    assert model_create.feature_set_hash == cli._feature_set_hash(feature)
    assert model_create.random_seed == 0
    assert model_create.target_kind == "excess_return_market"
    assert model_create.training_start_date < model_create.training_end_date
    assert model_create.test_start_date > model_create.training_end_date
    assert model_create.test_end_date >= model_create.test_start_date
    assert model_create.available_at_policy_versions == {"daily_price": 1}
    assert model_create.input_fingerprints["row_count"] == len(rows)
    assert "joined_feature_label_rows_sha256" in model_create.input_fingerprints
    assert model_create.parameters["feature_definition"]["definition_hash"] == "a" * 64
    assert model_create.parameters["window_source"] == "scorable_walk_forward"
    assert "output_path" not in model_create.parameters
    assert model_create.cost_assumptions["round_trip_cost_bps"] == 20.0

    backtest_create = repo.backtest_creates[0]
    assert backtest_create.model_run_id == 1
    assert backtest_create.target_kind == "excess_return_market"
    assert backtest_create.parameters["model_run_key"] == model_create.model_run_key
    assert backtest_create.multiple_comparisons_correction == "none"

    _model_run_id, model_finish = repo.model_finishes[0]
    assert model_finish.status == "succeeded"
    assert model_finish.metrics["split_count"] > 0
    assert model_finish.metrics["status"] == "succeeded"

    _backtest_run_id, backtest_finish = repo.backtest_finishes[0]
    assert backtest_finish.status == "succeeded"
    assert backtest_finish.metrics["mean_strategy_net_horizon_return"] is not None
    assert backtest_finish.metrics_by_regime
    assert "equal_weight_universe" in backtest_finish.baseline_metrics
    assert backtest_finish.label_scramble_metrics["status"] == "completed"
    assert backtest_finish.label_scramble_pass is True
    assert backtest_finish.multiple_comparisons_correction == "none"

    report_text = (tmp_path / "report.md").read_text(encoding="utf-8")
    assert "| model_run_id | 1 |" in report_text
    assert "| backtest_run_id | 2 |" in report_text
    assert (
        "| Model training window | "
        f"{model_create.training_start_date.isoformat()} to "
        f"{model_create.training_end_date.isoformat()} |"
    ) in report_text
    assert (
        "| Model test window | "
        f"{model_create.test_start_date.isoformat()} to "
        f"{model_create.test_end_date.isoformat()} |"
    ) in report_text
    assert "| Target kind | excess_return_market |" in report_text
    assert "| Random seed | 0 |" in report_text
    assert '"label_scramble_seed":44' in report_text
    assert '"min_train_sessions":252' in report_text
    assert '"round_trip_cost_bps":20.0' in report_text
    assert "| Report schema version | 3 |" in report_text
    assert "| covid_shock_recovery_2020_2021 |" in report_text
    assert "| Scored-row source | reported_scored_walk_forward_test_rows |" in report_text
    assert "| Selection rule | reported_top_half_selection_mask |" in report_text
    assert "| P-value | 0.010000 |" in report_text
    assert "| Pass/fail | pass |" in report_text
    assert "| Multiple-comparisons correction | none |" in report_text
    assert "No regime evidence supplied" not in report_text
    assert "No label-scramble evidence supplied" not in report_text


def test_load_policy_versions_uses_joined_feature_and_label_rows() -> None:
    client = FakePolicyVersionClient(
        {
            "policy_versions": {"daily_price": 1, "benchmark_price": 2},
            "policy_name_count": 2,
            "policy_pair_count": 2,
        }
    )

    policies = cli._load_policy_versions(
        client,
        feature_definition_id=17,
        horizon=63,
        universe="falsifier_seed",
    )

    assert policies == {"benchmark_price": 2, "daily_price": 1}
    sql = client.sql
    assert "fv.available_at_policy_id" in sql
    assert "frl.available_at_policy_id" in sql
    assert "WHERE fv.feature_definition_id = 17" in sql
    assert "frl.horizon_days = 63" in sql
    assert "um.universe_name = 'falsifier_seed'" in sql
    assert "FROM silver.available_at_policies\nWHERE valid_to IS NULL" not in sql


def test_load_policy_versions_rejects_conflicting_versions_for_same_policy() -> None:
    client = FakePolicyVersionClient(
        {
            "policy_versions": {"daily_price": 2},
            "policy_name_count": 1,
            "policy_pair_count": 2,
        }
    )

    with pytest.raises(cli.FalsifierCliError, match="conflicting available_at"):
        cli._load_policy_versions(
            client,
            feature_definition_id=17,
            horizon=63,
            universe="falsifier_seed",
        )


def test_model_run_create_uses_stable_key_for_same_frozen_metadata(
    tmp_path: Path,
) -> None:
    calendar = _calendar()
    rows = _momentum_rows(calendar, session_count=420)
    persisted_inputs = _persisted_inputs(rows=rows)
    args = cli.parse_args(
        [
            "--database-url",
            "postgresql://user:pass@localhost/silver",
            "--output-path",
            str(tmp_path / "report.md"),
        ]
    )
    feature_set_hash = cli._feature_set_hash(persisted_inputs.feature_definition)
    input_fingerprint = cli.fingerprint_momentum_inputs(rows)
    data_coverage = cli.coverage_from_rows(rows)
    model_window = cli._model_run_window(
        persisted_inputs.rows,
        calendar=calendar,
        horizon=args.horizon,
    )

    first = cli._model_run_create(
        args,
        persisted_inputs=persisted_inputs,
        feature_set_hash=feature_set_hash,
        git_sha="abcdef0",
        input_fingerprint=input_fingerprint,
        data_coverage=data_coverage,
        window=model_window,
    )
    second = cli._model_run_create(
        args,
        persisted_inputs=persisted_inputs,
        feature_set_hash=feature_set_hash,
        git_sha="abcdef0",
        input_fingerprint=input_fingerprint,
        data_coverage=data_coverage,
        window=model_window,
    )
    changed_input = cli._model_run_create(
        args,
        persisted_inputs=persisted_inputs,
        feature_set_hash=feature_set_hash,
        git_sha="abcdef0",
        input_fingerprint="f" * 64,
        data_coverage=data_coverage,
        window=model_window,
    )
    changed_surrogate_id_inputs = cli.PersistedFalsifierInputs(
        universe_members=persisted_inputs.universe_members,
        feature_definition=cli.FeatureDefinitionRecord(
            id=999,
            name=persisted_inputs.feature_definition.name,
            version=persisted_inputs.feature_definition.version,
            definition_hash=persisted_inputs.feature_definition.definition_hash,
        ),
        rows=persisted_inputs.rows,
        available_at_policy_versions=persisted_inputs.available_at_policy_versions,
        target_kind=persisted_inputs.target_kind,
    )
    changed_surrogate_id = cli._model_run_create(
        args,
        persisted_inputs=changed_surrogate_id_inputs,
        feature_set_hash=feature_set_hash,
        git_sha="abcdef0",
        input_fingerprint=input_fingerprint,
        data_coverage=data_coverage,
        window=model_window,
    )

    assert first.model_run_key == second.model_run_key
    assert first.parameters == second.parameters
    assert first.model_run_key != changed_input.model_run_key
    assert first.model_run_key == changed_surrogate_id.model_run_key
    assert first.parameters == changed_surrogate_id.parameters


def test_deterministic_run_payloads_ignore_invocation_output_path(
    tmp_path: Path,
) -> None:
    calendar = _calendar()
    rows = _momentum_rows(calendar, session_count=420)
    persisted_inputs = _persisted_inputs(rows=rows)
    first_args = cli.parse_args(
        [
            "--database-url",
            "postgresql://user:pass@localhost/silver",
            "--output-path",
            str(tmp_path / "first.md"),
        ]
    )
    second_args = cli.parse_args(
        [
            "--database-url",
            "postgresql://user:pass@localhost/silver",
            "--output-path",
            str(tmp_path / "second.md"),
        ]
    )
    feature_set_hash = cli._feature_set_hash(persisted_inputs.feature_definition)
    input_fingerprint = cli.fingerprint_momentum_inputs(rows)
    data_coverage = cli.coverage_from_rows(rows)
    model_window = cli._model_run_window(
        persisted_inputs.rows,
        calendar=calendar,
        horizon=first_args.horizon,
    )

    first_model = cli._model_run_create(
        first_args,
        persisted_inputs=persisted_inputs,
        feature_set_hash=feature_set_hash,
        git_sha="abcdef0",
        input_fingerprint=input_fingerprint,
        data_coverage=data_coverage,
        window=model_window,
    )
    second_model = cli._model_run_create(
        second_args,
        persisted_inputs=persisted_inputs,
        feature_set_hash=feature_set_hash,
        git_sha="abcdef0",
        input_fingerprint=input_fingerprint,
        data_coverage=data_coverage,
        window=model_window,
    )
    model_record = cli.ModelRunRecord(
        id=1,
        model_run_key=first_model.model_run_key,
        status="running",
    )
    first_backtest = cli._backtest_run_create(
        first_args,
        model_run=model_record,
        persisted_inputs=persisted_inputs,
    )
    second_backtest = cli._backtest_run_create(
        second_args,
        model_run=model_record,
        persisted_inputs=persisted_inputs,
    )

    assert first_model.model_run_key == second_model.model_run_key
    assert first_model.parameters == second_model.parameters
    assert first_backtest.backtest_run_key == second_backtest.backtest_run_key
    assert first_backtest.parameters == second_backtest.parameters
    assert "output_path" not in first_backtest.parameters


def test_report_run_records_invocation_metadata_outside_deterministic_keys(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calendar = _calendar()
    rows = _momentum_rows(calendar, session_count=420)
    repo = FakeMetadataRepository()
    invocation_repo = FakeInvocationRepository()
    args = cli.parse_args(
        [
            "--database-url",
            "postgresql://user:pass@localhost/silver",
            "--output-path",
            str(tmp_path / "report.md"),
        ]
    )
    invocation_id = UUID("12345678-1234-5678-1234-567812345678")
    monkeypatch.setattr(cli, "_git_sha", lambda: "abcdef0")
    monkeypatch.setattr(cli.uuid, "uuid4", lambda: invocation_id)
    monkeypatch.setattr(cli.os, "getpid", lambda: 4321)
    monkeypatch.setattr(cli, "run_label_scramble", _fake_label_scramble)
    monkeypatch.setattr(
        cli,
        "load_persisted_inputs",
        lambda *_args, **_kwargs: _persisted_inputs(rows=rows),
    )

    outcome = cli.run_report_with_metadata(
        args,
        client=object(),
        metadata_repository=repo,
        invocation_repository=invocation_repo,
        calendar=calendar,
    )

    assert outcome.status == "succeeded"
    assert len(invocation_repo.creates) == 1
    assert invocation_repo.finishes == [(7, "succeeded")]
    invocation = invocation_repo.creates[0]
    invocation_parameters = invocation["parameters"]
    assert invocation["run_kind"] == "falsifier_report_invocation"
    assert invocation["code_git_sha"] == "abcdef0"
    assert invocation["random_seed"] == 0
    assert invocation["input_fingerprints"] == {
        "joined_feature_label_rows_sha256": repo.model_creates[0].input_fingerprints[
            "joined_feature_label_rows_sha256"
        ],
    }
    assert invocation_parameters["invocation_id"] == str(invocation_id)
    assert invocation_parameters["process_id"] == 4321
    assert invocation_parameters["output_path"] == cli._display_path(args.output_path)
    assert invocation_parameters["model_run_key"] == repo.model_creates[0].model_run_key
    assert (
        invocation_parameters["backtest_run_key"]
        == repo.backtest_creates[0].backtest_run_key
    )
    assert str(invocation_id) not in repo.model_creates[0].model_run_key
    assert "invocation_id" not in repo.model_creates[0].parameters
    assert "output_path" not in repo.backtest_creates[0].parameters


def test_deterministic_rerun_reuses_terminal_metadata_rows() -> None:
    repo = FakeMetadataRepository()
    model = cli.ModelRunRecord(
        id=1,
        model_run_key="model-run-1",
        status="succeeded",
    )
    backtest = cli.BacktestRunRecord(
        id=2,
        backtest_run_key="backtest-run-1",
        status="succeeded",
    )

    reused_model = cli._finish_or_reuse_model_run(
        repo,
        model_run=model,
        finish=cli.ModelRunFinish(status="succeeded", metrics={"split_count": 3}),
    )
    reused_backtest = cli._finish_or_reuse_backtest_run(
        repo,
        backtest_run=backtest,
        finish=cli.BacktestRunFinish(
            status="succeeded",
            cost_assumptions={"round_trip_cost_bps": 20.0},
            metrics={"status": "succeeded"},
            metrics_by_regime={"all": {"count": 1}},
            baseline_metrics={"equal_weight": {"mean_net_horizon_return": 0.01}},
            label_scramble_metrics={"p_value": 0.01},
            label_scramble_pass=True,
            multiple_comparisons_correction="none",
        ),
    )

    assert reused_model == model
    assert reused_backtest == backtest
    assert repo.model_finishes == []
    assert repo.backtest_finishes == []


def test_report_traceability_validation_fails_clearly_on_metadata_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calendar = _calendar()
    repo = FakeMetadataRepository(
        traceability_overrides={"model_code_git_sha": "badcafe"},
    )
    args = cli.parse_args(
        [
            "--database-url",
            "postgresql://user:pass@localhost/silver",
            "--output-path",
            str(tmp_path / "report.md"),
        ]
    )
    monkeypatch.setattr(cli, "_git_sha", lambda: "abcdef0")
    monkeypatch.setattr(cli, "run_label_scramble", _fake_label_scramble)
    monkeypatch.setattr(
        cli,
        "load_persisted_inputs",
        lambda *_args, **_kwargs: _persisted_inputs(
            rows=_momentum_rows(calendar, session_count=420),
        ),
    )

    with pytest.raises(
        cli.FalsifierCliError,
        match="model_runs.code_git_sha",
    ):
        cli.run_report_with_metadata(
            args,
            client=object(),
            metadata_repository=repo,
            calendar=calendar,
        )

    assert repo.traceability_loads == [2]
    assert not (tmp_path / "report.md").exists()


def test_report_traceability_validation_rejects_backtest_joined_to_wrong_model_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calendar = _calendar()
    repo = FakeMetadataRepository(
        traceability_overrides={"backtest_model_run_id": 999},
    )
    args = cli.parse_args(
        [
            "--database-url",
            "postgresql://user:pass@localhost/silver",
            "--output-path",
            str(tmp_path / "report.md"),
        ]
    )
    monkeypatch.setattr(cli, "_git_sha", lambda: "abcdef0")
    monkeypatch.setattr(cli, "run_label_scramble", _fake_label_scramble)
    monkeypatch.setattr(
        cli,
        "load_persisted_inputs",
        lambda *_args, **_kwargs: _persisted_inputs(
            rows=_momentum_rows(calendar, session_count=420),
        ),
    )

    with pytest.raises(
        cli.FalsifierCliError,
        match="backtest_runs.model_run_id",
    ):
        cli.run_report_with_metadata(
            args,
            client=object(),
            metadata_repository=repo,
            calendar=calendar,
        )

    assert repo.traceability_loads == [2]
    assert not (tmp_path / "report.md").exists()


@pytest.mark.parametrize(
    ("traceability_overrides", "expected_field"),
    (
        (
            {"model_available_at_policy_versions": {"daily_price": 2}},
            "model_runs.available_at_policy_versions",
        ),
        (
            {
                "model_input_fingerprints": {
                    "joined_feature_label_rows_sha256": "e" * 64,
                },
            },
            "model_runs.input_fingerprints.joined_feature_label_rows_sha256",
        ),
        (
            {"model_cost_assumptions": {"round_trip_cost_bps": 99.0}},
            "model_runs.cost_assumptions",
        ),
        (
            {"model_parameters": {"window_source": "manual_override"}},
            "model_runs.parameters.window_source",
        ),
        (
            {"backtest_universe_name": "expanded_seed"},
            "backtest_runs.universe_name",
        ),
        (
            {"backtest_cost_assumptions": {"round_trip_cost_bps": 99.0}},
            "backtest_runs.cost_assumptions",
        ),
        ({"backtest_metrics_by_regime": {}}, "backtest_runs.metrics_by_regime"),
        (
            {"backtest_label_scramble_metrics": {"status": "not_run"}},
            "backtest_runs.label_scramble_metrics",
        ),
        ({"backtest_label_scramble_pass": False}, "backtest_runs.label_scramble_pass"),
        (
            {"backtest_multiple_comparisons_correction": "bh"},
            "backtest_runs.multiple_comparisons_correction",
        ),
    ),
)
def test_report_traceability_validation_checks_complete_backtest_claim_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    traceability_overrides: dict[str, Any],
    expected_field: str,
) -> None:
    calendar = _calendar()
    repo = FakeMetadataRepository(traceability_overrides=traceability_overrides)
    args = cli.parse_args(
        [
            "--database-url",
            "postgresql://user:pass@localhost/silver",
            "--output-path",
            str(tmp_path / "report.md"),
        ]
    )
    monkeypatch.setattr(cli, "_git_sha", lambda: "abcdef0")
    monkeypatch.setattr(cli, "run_label_scramble", _fake_label_scramble)
    monkeypatch.setattr(
        cli,
        "load_persisted_inputs",
        lambda *_args, **_kwargs: _persisted_inputs(
            rows=_momentum_rows(calendar, session_count=420),
        ),
    )

    with pytest.raises(cli.FalsifierCliError, match=expected_field):
        cli.run_report_with_metadata(
            args,
            client=object(),
            metadata_repository=repo,
            calendar=calendar,
        )

    assert repo.traceability_loads == [2]
    assert not (tmp_path / "report.md").exists()


def test_load_falsifier_replay_plan_normalizes_stored_claim_by_key() -> None:
    snapshot = _replay_snapshot()
    repo = FakeReplayMetadataRepository(snapshot)

    plan = cli.load_falsifier_replay_plan(
        repo,
        backtest_run_key="backtest-run-1",
    )

    assert repo.lookups == [
        {"backtest_run_id": None, "backtest_run_key": "backtest-run-1"},
    ]
    assert plan.backtest_run_id == 2
    assert plan.backtest_run_key == "backtest-run-1"
    assert plan.model_run_id == 1
    assert plan.model_run_key == "model-run-1"
    assert plan.strategy == cli.TARGET_STRATEGY
    assert plan.horizon == 63
    assert plan.universe == "falsifier_seed"
    assert plan.target_kind == "excess_return_market"
    assert plan.feature_set_hash == "a" * 64
    assert plan.input_fingerprint == "f" * 64
    assert plan.available_at_policy_versions == {"daily_price": 1}
    assert plan.model_window == cli.FalsifierModelWindow(
        training_start_date=date(2020, 1, 2),
        training_end_date=date(2021, 12, 31),
        test_start_date=date(2022, 1, 3),
        test_end_date=date(2024, 12, 31),
        source="scorable_walk_forward",
    )
    assert plan.execution_assumptions == cli._execution_assumptions()


def test_load_falsifier_replay_plan_rejects_non_accepted_claim() -> None:
    snapshot = _replay_snapshot(backtest_status="running")
    repo = FakeReplayMetadataRepository(snapshot)

    with pytest.raises(
        cli.FalsifierCliError,
        match="backtest_runs.status expected 'succeeded' got 'running'",
    ):
        cli.load_falsifier_replay_plan(repo, backtest_run_id=2)


def test_falsifier_replay_snapshot_validation_reports_metric_mismatch() -> None:
    stored = _replay_snapshot()
    replayed = replace(
        stored,
        backtest_metrics={"status": "succeeded", "mean_strategy_net": 0.99},
    )

    comparison = cli.compare_falsifier_replay_snapshots(stored, replayed)

    assert not comparison.matches
    assert comparison.mismatches == (
        "backtest_runs.metrics expected "
        "{\"mean_strategy_net\":0.018,\"status\":\"succeeded\"} got "
        "{\"mean_strategy_net\":0.99,\"status\":\"succeeded\"}",
    )
    with pytest.raises(cli.FalsifierCliError, match="backtest_runs.metrics"):
        cli.validate_falsifier_replay_snapshots(stored, replayed)


def test_replay_dry_run_prints_loaded_identity_and_skips_rerun() -> None:
    snapshot = _replay_snapshot()
    repo = FakeReplayMetadataRepository(snapshot)
    args = cli.parse_args(
        [
            "--database-url",
            "postgresql://user:pass@localhost/silver",
            "--replay-backtest-run-id",
            "2",
            "--replay-dry-run",
        ]
    )

    plan = cli.run_replay_dry_run_with_metadata(
        args,
        metadata_repository=repo,
    )
    output = cli.render_falsifier_replay_dry_run(plan)

    assert repo.lookups == [{"backtest_run_id": 2, "backtest_run_key": None}]
    assert "OK: falsifier replay dry-run loaded accepted claim metadata" in output
    assert "model_run_id=1" in output
    assert "backtest_run_id=2" in output
    assert (
        "Replay command: python scripts/run_falsifier.py "
        "--replay-backtest-run-id 2"
    ) in output
    assert (
        "Resolved run command: python scripts/run_falsifier.py "
        "--strategy momentum_12_1 --horizon 63 --universe falsifier_seed"
    ) in output
    assert "Evidence: stored accepted-claim metadata matched replay contract" in output
    assert "rerun not executed (--replay-dry-run)" in output


def test_replay_run_uses_stored_plan_and_prints_match_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _replay_snapshot()
    repo = FakeReplayMetadataRepository(snapshot)
    output_path = Path("reports/falsifier/replay.md")
    args = cli.parse_args(
        [
            "--database-url",
            "postgresql://user:pass@localhost/silver",
            "--replay-backtest-run-key",
            "backtest-run-1",
            "--output-path",
            str(output_path),
        ]
    )
    captured: dict[str, Any] = {}

    def fake_run_report_with_metadata(
        replay_args: Any,
        **_kwargs: Any,
    ) -> Any:
        captured["args"] = replay_args
        return cli.FalsifierReportRun(
            model_run=cli.ModelRunRecord(
                id=1,
                model_run_key="model-run-1",
                status="succeeded",
            ),
            backtest_run=cli.BacktestRunRecord(
                id=2,
                backtest_run_key="backtest-run-1",
                status="succeeded",
            ),
            status="succeeded",
        )

    monkeypatch.setattr(cli, "run_report_with_metadata", fake_run_report_with_metadata)

    replay = cli.run_replay_with_metadata(
        args,
        client=object(),
        metadata_repository=repo,
        calendar=object(),
    )
    output = cli.render_falsifier_replay_result(replay, output_path=args.output_path)

    assert repo.lookups == [
        {"backtest_run_id": None, "backtest_run_key": "backtest-run-1"},
        {"backtest_run_id": 2, "backtest_run_key": None},
    ]
    assert captured["args"].strategy == cli.TARGET_STRATEGY
    assert captured["args"].horizon == 63
    assert captured["args"].universe == "falsifier_seed"
    assert captured["args"].output_path == args.output_path
    assert replay.comparison.matches
    assert "OK: falsifier replay matched stored metadata" in output
    assert "stored_backtest_run_id=2" in output
    assert "replayed_backtest_run_id=2" in output
    assert "Evidence: all replay-critical identity and metric fields matched" in output
    assert f"Report: {cli._display_path(args.output_path)}" in output


def test_replay_run_fails_with_mismatch_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = _replay_snapshot()
    replayed = replace(
        stored,
        backtest_metrics={"status": "succeeded", "mean_strategy_net": 0.99},
    )
    repo = FakeReplayMetadataRepository(stored, replayed_snapshot=replayed)
    output_path = Path("reports/falsifier/replay.md")
    args = cli.parse_args(
        [
            "--database-url",
            "postgresql://user:pass@localhost/silver",
            "--replay-backtest-run-id",
            "2",
            "--output-path",
            str(output_path),
        ]
    )

    def fake_run_report_with_metadata(*_args: Any, **_kwargs: Any) -> Any:
        return cli.FalsifierReportRun(
            model_run=cli.ModelRunRecord(
                id=1,
                model_run_key="model-run-1",
                status="succeeded",
            ),
            backtest_run=cli.BacktestRunRecord(
                id=2,
                backtest_run_key="backtest-run-1",
                status="succeeded",
            ),
            status="succeeded",
        )

    monkeypatch.setattr(cli, "run_report_with_metadata", fake_run_report_with_metadata)

    with pytest.raises(
        cli.FalsifierCliError,
        match="falsifier replay mismatch.*backtest_runs.metrics",
    ):
        cli.run_replay_with_metadata(
            args,
            client=object(),
            metadata_repository=repo,
            calendar=object(),
        )


def test_label_scramble_payload_uses_scored_strategy_test_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = _calendar()
    rows = _momentum_rows(calendar, session_count=420)
    result = cli.run_momentum_falsifier(
        rows,
        calendar=calendar,
        horizon_sessions=63,
        min_train_sessions=cli.DEFAULT_MIN_TRAIN_SESSIONS,
        test_sessions=cli.DEFAULT_TEST_SESSIONS,
        step_sessions=cli.DEFAULT_STEP_SESSIONS,
        round_trip_cost_bps=cli.DEFAULT_ROUND_TRIP_COST_BPS,
    )
    assert result.status == "succeeded"
    scored_dates = tuple(
        date_result.asof_date for date_result in cli._date_results(result)
    )
    assert scored_dates
    assert rows[0].asof_date not in set(scored_dates)
    captured: dict[str, Any] = {}

    def capture_label_scramble(
        samples: tuple[Any, ...],
        *,
        seed: int,
        trial_count: int,
        scoring_function: Any | None = None,
        **_kwargs: Any,
    ) -> FakeLabelScrambleResult:
        captured["samples"] = samples
        captured["seed"] = seed
        captured["trial_count"] = trial_count
        captured["scoring_function"] = scoring_function
        return FakeLabelScrambleResult()

    monkeypatch.setattr(cli, "run_label_scramble", capture_label_scramble)

    metrics, _passed = cli._label_scramble_payload(rows=rows, result=result)

    samples = captured["samples"]
    sample_dates = {date.fromisoformat(sample.group_key) for sample in samples}
    assert sample_dates == set(scored_dates)
    assert len(samples) == result.headline_metrics.eligible_observations
    assert metrics["scored_row_source"] == "reported_scored_walk_forward_test_rows"
    assert metrics["scored_test_dates"] == result.headline_metrics.scored_test_dates
    assert metrics["eligible_observations"] == (
        result.headline_metrics.eligible_observations
    )
    assert metrics["selected_observations"] == (
        result.headline_metrics.selected_observations
    )
    assert captured["seed"] == cli.DEFAULT_LABEL_SCRAMBLE_SEED
    assert captured["trial_count"] == cli.DEFAULT_LABEL_SCRAMBLE_TRIAL_COUNT
    assert captured["scoring_function"] is not None


def test_label_scramble_selection_mask_matches_reported_strategy_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scored_date = date(2024, 1, 2)
    horizon_date = date(2024, 4, 1)
    rows = (
        cli.MomentumBacktestRow(
            ticker="HIGH",
            asof_date=scored_date,
            horizon_date=horizon_date,
            feature_value=10.0,
            realized_return=0.01,
        ),
        cli.MomentumBacktestRow(
            ticker="LOW",
            asof_date=scored_date,
            horizon_date=horizon_date,
            feature_value=1.0,
            realized_return=0.20,
        ),
    )
    result = _single_date_falsifier_result(
        scored_date=scored_date,
        selected_tickers=("LOW",),
        strategy_gross_return=0.20,
        baseline_gross_return=0.105,
    )
    captured: dict[str, Any] = {}

    def capture_label_scramble(
        samples: tuple[Any, ...],
        *,
        scoring_function: Any | None = None,
        **_kwargs: Any,
    ) -> FakeLabelScrambleResult:
        captured["samples"] = samples
        captured["scoring_function"] = scoring_function
        return FakeLabelScrambleResult()

    monkeypatch.setattr(cli, "run_label_scramble", capture_label_scramble)

    metrics, _passed = cli._label_scramble_payload(rows=rows, result=result)

    samples_by_id = {sample.sample_id: sample for sample in captured["samples"]}
    assert samples_by_id["LOW-2024-01-02"].feature_value == 1.0
    assert samples_by_id["HIGH-2024-01-02"].feature_value == 0.0
    assert captured["scoring_function"] is not None
    assert captured["scoring_function"](captured["samples"]) == pytest.approx(0.198)
    assert metrics["selection_rule"] == "reported_top_half_selection_mask"
    assert metrics["reported_mean_strategy_net_horizon_return"] == pytest.approx(0.198)


def test_report_run_finishes_model_and_backtest_as_insufficient_data(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calendar = _calendar()
    repo = FakeMetadataRepository()
    args = cli.parse_args(
        [
            "--database-url",
            "postgresql://user:pass@localhost/silver",
            "--output-path",
            str(tmp_path / "report.md"),
        ]
    )
    monkeypatch.setattr(cli, "_git_sha", lambda: "abcdef0")
    monkeypatch.setattr(cli, "run_label_scramble", _fake_label_scramble)
    monkeypatch.setattr(
        cli,
        "load_persisted_inputs",
        lambda *_args, **_kwargs: _persisted_inputs(
            rows=_momentum_rows(calendar, session_count=1),
        ),
    )

    outcome = cli.run_report_with_metadata(
        args,
        client=object(),
        metadata_repository=repo,
        calendar=calendar,
    )

    assert outcome.status == "insufficient_data"
    assert repo.model_finishes[0][1].status == "insufficient_data"
    assert repo.model_finishes[0][1].metrics["split_count"] == 0
    assert repo.model_finishes[0][1].metrics["failure_modes"]
    assert repo.backtest_finishes[0][1].status == "insufficient_data"
    assert repo.backtest_finishes[0][1].metrics["status"] == "insufficient_data"
    assert repo.backtest_finishes[0][1].label_scramble_pass is False
    assert repo.model_creates[0].parameters["window_source"] == "input_coverage_fallback"


def test_report_run_finishes_model_and_backtest_as_failed_on_execution_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calendar = _calendar()
    repo = FakeMetadataRepository()
    args = cli.parse_args(
        [
            "--database-url",
            "postgresql://user:pass@localhost/silver",
            "--output-path",
            str(tmp_path / "report.md"),
        ]
    )
    monkeypatch.setattr(cli, "_git_sha", lambda: "abcdef0")
    monkeypatch.setattr(
        cli,
        "load_persisted_inputs",
        lambda *_args, **_kwargs: _persisted_inputs(
            rows=_momentum_rows(calendar, session_count=420),
        ),
    )

    def fail_run(*_args: object, **_kwargs: object) -> object:
        raise cli.MomentumFalsifierInputError("synthetic execution failure")

    monkeypatch.setattr(cli, "run_momentum_falsifier", fail_run)

    with pytest.raises(cli.MomentumFalsifierInputError):
        cli.run_report_with_metadata(
            args,
            client=object(),
            metadata_repository=repo,
            calendar=calendar,
        )

    assert repo.model_finishes[0][1].status == "failed"
    assert repo.model_finishes[0][1].metrics == {
        "error_message": "synthetic execution failure",
        "error_type": "MomentumFalsifierInputError",
    }
    assert repo.backtest_finishes[0][1].status == "failed"
    assert repo.backtest_finishes[0][1].metrics == {
        "status": "failed",
        "failure_message": "synthetic execution failure",
    }
    assert repo.backtest_finishes[0][1].label_scramble_pass is False
    assert not (tmp_path / "report.md").exists()


def _calendar() -> Any:
    return cli.TradingCalendar(cli.load_seed_csv(cli.DEFAULT_TRADING_CALENDAR_SEED_PATH))


def _feature_definition() -> Any:
    return cli.FeatureDefinitionRecord(
        id=17,
        name="momentum_12_1",
        version=1,
        definition_hash="a" * 64,
    )


def _persisted_inputs(rows: tuple[Any, ...]) -> Any:
    return cli.PersistedFalsifierInputs(
        universe_members=(
            cli.UniverseMember(
                ticker="AAA",
                valid_from=date(2020, 1, 2),
                valid_to=None,
            ),
            cli.UniverseMember(
                ticker="BBB",
                valid_from=date(2020, 1, 2),
                valid_to=None,
            ),
        ),
        feature_definition=_feature_definition(),
        rows=rows,
        available_at_policy_versions={"daily_price": 1},
        target_kind="excess_return_market",
    )


def _momentum_rows(calendar: Any, *, session_count: int) -> tuple[Any, ...]:
    sessions = [
        row.date
        for row in calendar.rows
        if row.is_session and date(2020, 1, 2) <= row.date <= date(2023, 12, 29)
    ][:session_count]
    rows = []
    for index, session in enumerate(sessions):
        horizon_date = calendar.advance_trading_days(session, 63)
        rows.append(
            cli.MomentumBacktestRow(
                ticker="AAA",
                asof_date=session,
                horizon_date=horizon_date,
                feature_value=float(index + 2),
                realized_return=0.02,
            )
        )
        rows.append(
            cli.MomentumBacktestRow(
                ticker="BBB",
                asof_date=session,
                horizon_date=horizon_date,
                feature_value=float(index + 1),
                realized_return=0.01,
            )
        )
    return tuple(rows)


class FakeLabelScrambleResult:
    p_value = 0.01

    def to_dict(self) -> dict[str, object]:
        return {
            "p_value": self.p_value,
            "seed": cli.DEFAULT_LABEL_SCRAMBLE_SEED,
            "trial_count": cli.DEFAULT_LABEL_SCRAMBLE_TRIAL_COUNT,
        }


def _fake_label_scramble(*_args: Any, **_kwargs: Any) -> FakeLabelScrambleResult:
    return FakeLabelScrambleResult()


def _single_date_falsifier_result(
    *,
    scored_date: date,
    selected_tickers: tuple[str, ...],
    strategy_gross_return: float,
    baseline_gross_return: float,
) -> Any:
    cost_fraction = cli.DEFAULT_ROUND_TRIP_COST_BPS / 10_000.0
    strategy_net_return = strategy_gross_return - cost_fraction
    baseline_net_return = baseline_gross_return - cost_fraction
    date_result = cli.MomentumDateResult(
        asof_date=scored_date,
        eligible_count=2,
        selected_tickers=selected_tickers,
        strategy_gross_return=strategy_gross_return,
        strategy_net_return=strategy_net_return,
        baseline_gross_return=baseline_gross_return,
        baseline_net_return=baseline_net_return,
    )
    return MomentumFalsifierResult(
        status="succeeded",
        horizon_sessions=63,
        round_trip_cost_bps=cli.DEFAULT_ROUND_TRIP_COST_BPS,
        min_train_sessions=cli.DEFAULT_MIN_TRAIN_SESSIONS,
        test_sessions=cli.DEFAULT_TEST_SESSIONS,
        step_sessions=cli.DEFAULT_STEP_SESSIONS,
        windows=(
            MomentumWindowResult(
                split_index=0,
                train_start=scored_date,
                train_end=scored_date,
                test_start=scored_date,
                test_end=scored_date,
                train_observations=2,
                test_observations=2,
                scored_dates=1,
                selected_observations=len(selected_tickers),
                strategy_net_return=strategy_net_return,
                baseline_net_return=baseline_net_return,
                date_results=(date_result,),
            ),
        ),
        headline_metrics=MomentumHeadlineMetrics(
            split_count=1,
            scored_test_dates=1,
            eligible_observations=2,
            selected_observations=len(selected_tickers),
            mean_strategy_gross_return=strategy_gross_return,
            mean_strategy_net_return=strategy_net_return,
            mean_baseline_gross_return=baseline_gross_return,
            mean_baseline_net_return=baseline_net_return,
            mean_net_difference_vs_baseline=strategy_net_return - baseline_net_return,
            strategy_net_hit_rate=1.0,
            strategy_net_return_stddev=None,
            strategy_net_return_to_stddev=None,
        ),
        failure_modes=(),
    )


class FakeMetadataRepository:
    def __init__(
        self,
        *,
        traceability_overrides: dict[str, Any] | None = None,
    ) -> None:
        self.model_creates: list[Any] = []
        self.model_finishes: list[tuple[int, Any]] = []
        self.backtest_creates: list[Any] = []
        self.backtest_finishes: list[tuple[int, Any]] = []
        self.traceability_loads: list[int] = []
        self.traceability_overrides = traceability_overrides or {}

    def create_model_run(self, run: Any) -> Any:
        self.model_creates.append(run)
        return cli.ModelRunRecord(
            id=1,
            model_run_key=run.model_run_key,
            status="running",
        )

    def finish_model_run(self, run_id: int, finish: Any) -> Any:
        self.model_finishes.append((run_id, finish))
        return cli.ModelRunRecord(
            id=run_id,
            model_run_key=self.model_creates[0].model_run_key,
            status=finish.status,
        )

    def create_backtest_run(self, run: Any) -> Any:
        self.backtest_creates.append(run)
        return cli.BacktestRunRecord(
            id=2,
            backtest_run_key=run.backtest_run_key,
            status="running",
        )

    def finish_backtest_run(self, run_id: int, finish: Any) -> Any:
        self.backtest_finishes.append((run_id, finish))
        return cli.BacktestRunRecord(
            id=run_id,
            backtest_run_key=self.backtest_creates[0].backtest_run_key,
            status=finish.status,
        )

    def load_backtest_traceability_snapshot(self, backtest_run_id: int) -> Any:
        self.traceability_loads.append(backtest_run_id)
        model_create = self.model_creates[0]
        model_finish = self.model_finishes[0][1]
        backtest_create = self.backtest_creates[0]
        backtest_finish = self.backtest_finishes[0][1]
        values = {
            "model_run_id": 1,
            "model_run_key": model_create.model_run_key,
            "model_status": model_finish.status,
            "model_code_git_sha": model_create.code_git_sha,
            "model_feature_set_hash": model_create.feature_set_hash,
            "model_feature_snapshot_ref": model_create.feature_snapshot_ref,
            "model_training_start_date": model_create.training_start_date,
            "model_training_end_date": model_create.training_end_date,
            "model_test_start_date": model_create.test_start_date,
            "model_test_end_date": model_create.test_end_date,
            "model_horizon_days": model_create.horizon_days,
            "model_target_kind": model_create.target_kind,
            "model_random_seed": model_create.random_seed,
            "model_cost_assumptions": dict(model_create.cost_assumptions),
            "model_metrics": dict(model_finish.metrics),
            "model_parameters": dict(model_create.parameters),
            "model_available_at_policy_versions": dict(
                model_create.available_at_policy_versions,
            ),
            "model_input_fingerprints": dict(model_create.input_fingerprints),
            "backtest_run_id": 2,
            "backtest_run_key": backtest_create.backtest_run_key,
            "backtest_status": backtest_finish.status,
            "backtest_model_run_id": backtest_create.model_run_id,
            "backtest_universe_name": backtest_create.universe_name,
            "backtest_horizon_days": backtest_create.horizon_days,
            "backtest_target_kind": backtest_create.target_kind,
            "backtest_cost_assumptions": dict(backtest_finish.cost_assumptions),
            "backtest_metrics": dict(backtest_finish.metrics),
            "backtest_metrics_by_regime": dict(backtest_finish.metrics_by_regime),
            "backtest_baseline_metrics": dict(backtest_finish.baseline_metrics),
            "backtest_label_scramble_metrics": dict(
                backtest_finish.label_scramble_metrics,
            ),
            "backtest_label_scramble_pass": backtest_finish.label_scramble_pass,
            "backtest_parameters": dict(backtest_create.parameters),
            "backtest_multiple_comparisons_correction": (
                backtest_finish.multiple_comparisons_correction
            ),
        }
        values.update(self.traceability_overrides)
        return cli.BacktestTraceabilitySnapshot(**values)


class FakeReplayMetadataRepository:
    def __init__(self, snapshot: Any, *, replayed_snapshot: Any | None = None) -> None:
        self.snapshot = snapshot
        self.replayed_snapshot = replayed_snapshot or snapshot
        self.lookups: list[dict[str, object]] = []

    def load_backtest_replay_snapshot(
        self,
        *,
        backtest_run_id: int | None = None,
        backtest_run_key: str | None = None,
    ) -> Any:
        self.lookups.append(
            {
                "backtest_run_id": backtest_run_id,
                "backtest_run_key": backtest_run_key,
            }
        )
        if len(self.lookups) == 1:
            return self.snapshot
        return self.replayed_snapshot


class FakeInvocationRepository:
    def __init__(self) -> None:
        self.creates: list[dict[str, Any]] = []
        self.finishes: list[tuple[int, str]] = []

    def create_run(self, **kwargs: Any) -> Any:
        self.creates.append(kwargs)
        return cli.AnalyticsRunRecord(
            id=7,
            run_kind=kwargs["run_kind"],
            status="running",
        )

    def finish_run(self, run_id: int, *, status: str) -> Any:
        self.finishes.append((run_id, status))
        return cli.AnalyticsRunRecord(
            id=run_id,
            run_kind=self.creates[0]["run_kind"],
            status=status,
        )


class FakePolicyVersionClient:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.sql = ""

    def fetch_json(self, sql: str) -> Any:
        self.sql = sql
        return self.payload


def _replay_snapshot(**overrides: Any) -> Any:
    values = {
        "model_run_id": 1,
        "model_run_key": "model-run-1",
        "model_status": "succeeded",
        "model_code_git_sha": "abcdef0",
        "model_feature_set_hash": "a" * 64,
        "model_feature_snapshot_ref": None,
        "model_training_start_date": date(2020, 1, 2),
        "model_training_end_date": date(2021, 12, 31),
        "model_test_start_date": date(2022, 1, 3),
        "model_test_end_date": date(2024, 12, 31),
        "model_horizon_days": 63,
        "model_target_kind": "excess_return_market",
        "model_random_seed": cli.FALSIFIER_RANDOM_SEED,
        "model_cost_assumptions": cli._model_run_cost_assumptions(),
        "model_metrics": {"status": "succeeded", "mean_strategy_net": 0.018},
        "model_parameters": {
            "feature_definition": {
                "definition_hash": "a" * 64,
                "name": cli.TARGET_STRATEGY,
                "version": 1,
            },
            "min_train_sessions": cli.DEFAULT_MIN_TRAIN_SESSIONS,
            "step_sessions": cli.DEFAULT_STEP_SESSIONS,
            "strategy": cli.TARGET_STRATEGY,
            "test_sessions": cli.DEFAULT_TEST_SESSIONS,
            "universe": "falsifier_seed",
            "window_source": "scorable_walk_forward",
        },
        "model_available_at_policy_versions": {"daily_price": 1},
        "model_input_fingerprints": {
            "joined_feature_label_rows_sha256": "f" * 64,
            "row_count": 840,
        },
        "backtest_run_id": 2,
        "backtest_run_key": "backtest-run-1",
        "backtest_status": "succeeded",
        "backtest_model_run_id": 1,
        "backtest_universe_name": "falsifier_seed",
        "backtest_horizon_days": 63,
        "backtest_target_kind": "excess_return_market",
        "backtest_cost_assumptions": cli._cost_assumptions(None),
        "backtest_metrics": {"status": "succeeded", "mean_strategy_net": 0.018},
        "backtest_metrics_by_regime": {
            "pre_2019": {"sample_count": 12, "strategy_net_return": {"mean": 0.01}},
        },
        "backtest_baseline_metrics": {
            "equal_weight_universe": {"mean_net_horizon_return": 0.01},
        },
        "backtest_label_scramble_metrics": {
            "status": "completed",
            "p_value": 0.01,
        },
        "backtest_label_scramble_pass": True,
        "backtest_parameters": {
            "label_scramble_alpha": cli.LABEL_SCRAMBLE_ALPHA,
            "label_scramble_seed": cli.DEFAULT_LABEL_SCRAMBLE_SEED,
            "label_scramble_trial_count": cli.DEFAULT_LABEL_SCRAMBLE_TRIAL_COUNT,
            "metadata_role": "backtest_run",
            "model_run_key": "model-run-1",
            "multiple_comparisons_correction": (
                cli.MULTIPLE_COMPARISONS_CORRECTION
            ),
            "strategy": cli.TARGET_STRATEGY,
            "target_kind": "excess_return_market",
            "universe": "falsifier_seed",
        },
        "backtest_multiple_comparisons_correction": (
            cli.MULTIPLE_COMPARISONS_CORRECTION
        ),
    }
    values.update(overrides)
    return cli.BacktestTraceabilitySnapshot(**values)
