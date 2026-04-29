from __future__ import annotations

import importlib.util
import subprocess
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from silver.backtest.momentum_falsifier import (
    MomentumHeadlineMetrics,
    MomentumWindowResult,
)


ROOT = Path(__file__).resolve().parents[1]
RUN_FALSIFIER_SCRIPT = ROOT / "scripts" / "run_falsifier.py"


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


def test_run_report_persists_successful_backtest_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _load_script_module()
    rows = (
        module.MomentumBacktestRow("AAA", date(2024, 1, 3), date(2024, 4, 3), 1.0, 0.04),
        module.MomentumBacktestRow("BBB", date(2024, 1, 3), date(2024, 4, 3), 0.5, 0.01),
        module.MomentumBacktestRow("AAA", date(2024, 1, 4), date(2024, 4, 4), 1.2, 0.05),
        module.MomentumBacktestRow("BBB", date(2024, 1, 4), date(2024, 4, 4), 0.2, -0.01),
    )
    persisted = _persisted_inputs(module, rows=rows)
    result = module.MomentumFalsifierResult(
        status="succeeded",
        horizon_sessions=63,
        round_trip_cost_bps=20.0,
        min_train_sessions=252,
        test_sessions=63,
        step_sessions=63,
        windows=(
            MomentumWindowResult(
                split_index=1,
                train_start=date(2023, 1, 3),
                train_end=date(2023, 12, 29),
                test_start=date(2024, 1, 2),
                test_end=date(2024, 3, 29),
                train_observations=500,
                test_observations=4,
                scored_dates=2,
                selected_observations=2,
                strategy_net_return=0.035,
                baseline_net_return=0.01,
                date_results=(
                    module.MomentumDateResult(
                        asof_date=date(2024, 1, 3),
                        eligible_count=2,
                        selected_tickers=("AAA",),
                        strategy_gross_return=0.04,
                        strategy_net_return=0.038,
                        baseline_gross_return=0.025,
                        baseline_net_return=0.023,
                    ),
                    module.MomentumDateResult(
                        asof_date=date(2024, 1, 4),
                        eligible_count=2,
                        selected_tickers=("AAA",),
                        strategy_gross_return=0.05,
                        strategy_net_return=0.048,
                        baseline_gross_return=0.02,
                        baseline_net_return=0.018,
                    ),
                ),
            ),
        ),
        headline_metrics=MomentumHeadlineMetrics(
            split_count=1,
            scored_test_dates=2,
            eligible_observations=4,
            selected_observations=2,
            mean_strategy_gross_return=0.045,
            mean_strategy_net_return=0.043,
            mean_baseline_gross_return=0.0225,
            mean_baseline_net_return=0.0205,
            mean_net_difference_vs_baseline=0.0225,
            strategy_net_hit_rate=1.0,
            strategy_net_return_stddev=0.0070710678,
            strategy_net_return_to_stddev=6.0811183,
        ),
        failure_modes=(),
    )
    repo = RecordingMetadataRepository()
    connection = RecordingConnection()
    _install_run_report_fakes(
        module,
        monkeypatch,
        persisted=persisted,
        repo=repo,
        connection=connection,
        falsifier_result=result,
    )

    args = module.parse_args(
        [
            "--database-url",
            "postgresql://example/silver",
            "--output-path",
            str(tmp_path / "report.md"),
        ]
    )
    module.run_report(args)

    assert repo.model_create is not None
    assert repo.model_create.training_start_date == date(2023, 1, 3)
    assert repo.model_create.training_end_date == date(2023, 12, 29)
    assert repo.model_create.test_start_date == date(2024, 1, 2)
    assert repo.model_create.test_end_date == date(2024, 3, 29)

    assert repo.backtest_create is not None
    assert repo.backtest_create.model_run_id == 101
    assert repo.backtest_create.target_kind == "raw_return"

    assert repo.backtest_finish is not None
    assert repo.backtest_finish.status == "succeeded"
    assert repo.backtest_finish.cost_assumptions["round_trip_cost_bps"] == 20.0
    assert (
        repo.backtest_finish.metrics["mean_strategy_net_horizon_return"] == 0.043
    )
    assert repo.backtest_finish.baseline_metrics == {
        "equal_weight_universe": {
            "mean_gross_horizon_return": 0.0225,
            "mean_net_horizon_return": 0.0205,
        },
        "strategy_vs_equal_weight_universe": {
            "mean_net_difference": 0.0225,
        },
    }
    assert "recent_seed_window_2024_2026" in repo.backtest_finish.metrics_by_regime
    assert repo.backtest_finish.label_scramble_metrics["status"] == "completed"
    assert isinstance(repo.backtest_finish.label_scramble_pass, bool)
    assert repo.backtest_finish.multiple_comparisons_correction == "none"
    assert connection.committed is True
    assert connection.rolled_back is False

    report_text = (tmp_path / "report.md").read_text(encoding="utf-8")
    assert "| model_run_id | 101 |" in report_text
    assert "| backtest_run_id | 202 |" in report_text


def test_run_report_persists_insufficient_data_without_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _load_script_module()
    rows = (
        module.MomentumBacktestRow("AAA", date(2024, 1, 3), date(2024, 4, 3), 1.0, 0.04),
    )
    result = module.MomentumFalsifierResult(
        status="insufficient_data",
        horizon_sessions=63,
        round_trip_cost_bps=20.0,
        min_train_sessions=252,
        test_sessions=63,
        step_sessions=63,
        windows=(),
        headline_metrics=MomentumHeadlineMetrics(
            split_count=0,
            scored_test_dates=0,
            eligible_observations=0,
            selected_observations=0,
            mean_strategy_gross_return=None,
            mean_strategy_net_return=None,
            mean_baseline_gross_return=None,
            mean_baseline_net_return=None,
            mean_net_difference_vs_baseline=None,
            strategy_net_hit_rate=None,
            strategy_net_return_stddev=None,
            strategy_net_return_to_stddev=None,
        ),
        failure_modes=("Not enough covered trading sessions.",),
    )
    repo = RecordingMetadataRepository()
    _install_run_report_fakes(
        module,
        monkeypatch,
        persisted=_persisted_inputs(module, rows=rows),
        repo=repo,
        connection=RecordingConnection(),
        falsifier_result=result,
    )

    args = module.parse_args(
        [
            "--database-url",
            "postgresql://example/silver",
            "--output-path",
            str(tmp_path / "report.md"),
        ]
    )
    module.run_report(args)

    assert repo.model_create is not None
    assert repo.model_create.parameters["metadata_date_source"] == (
        "input_coverage_fallback"
    )
    assert repo.model_create.test_start_date > repo.model_create.training_end_date
    assert repo.model_finish is not None
    assert repo.model_finish.status == "insufficient_data"
    assert repo.backtest_finish is not None
    assert repo.backtest_finish.status == "insufficient_data"
    assert repo.backtest_finish.metrics["status"] == "insufficient_data"
    assert repo.backtest_finish.label_scramble_pass is False
    assert repo.backtest_finish.label_scramble_metrics["status"] == "not_run"


def test_run_report_finishes_metadata_as_failed_on_falsifier_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _load_script_module()
    rows = (
        module.MomentumBacktestRow("AAA", date(2024, 1, 3), date(2024, 4, 3), 1.0, 0.04),
        module.MomentumBacktestRow("BBB", date(2024, 1, 3), date(2024, 4, 3), 0.5, 0.01),
    )
    repo = RecordingMetadataRepository()
    connection = RecordingConnection()
    _install_run_report_fakes(
        module,
        monkeypatch,
        persisted=_persisted_inputs(module, rows=rows),
        repo=repo,
        connection=connection,
        falsifier_result=module.MomentumFalsifierInputError("duplicate input row"),
    )

    args = module.parse_args(
        [
            "--database-url",
            "postgresql://example/silver",
            "--output-path",
            str(tmp_path / "report.md"),
        ]
    )
    with pytest.raises(module.MomentumFalsifierInputError, match="duplicate input row"):
        module.run_report(args)

    assert repo.model_finish is not None
    assert repo.model_finish.status == "failed"
    assert repo.backtest_finish is not None
    assert repo.backtest_finish.status == "failed"
    assert repo.backtest_finish.metrics == {
        "status": "failed",
        "failure_message": "duplicate input row",
    }
    assert repo.backtest_finish.label_scramble_pass is False
    assert connection.committed is True
    assert connection.rolled_back is False
    assert not (tmp_path / "report.md").exists()


def _load_script_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "run_falsifier_under_test",
        RUN_FALSIFIER_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _persisted_inputs(
    module: Any,
    *,
    rows: tuple[Any, ...],
) -> Any:
    return module.PersistedFalsifierInputs(
        universe_members=(
            module.UniverseMember("AAA", date(2024, 1, 1), None),
            module.UniverseMember("BBB", date(2024, 1, 1), None),
        ),
        feature_definition=module.FeatureDefinitionRecord(
            id=7,
            name="momentum_12_1",
            version=1,
            definition_hash="b" * 64,
        ),
        rows=rows,
        target_kind="raw_return",
        available_at_policy_versions={"daily_price": 1},
    )


def _install_run_report_fakes(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
    *,
    persisted: Any,
    repo: "RecordingMetadataRepository",
    connection: "RecordingConnection",
    falsifier_result: Any,
) -> None:
    monkeypatch.setattr(
        module,
        "PsqlJsonClient",
        lambda *, database_url, psql_path=None: object(),
    )
    monkeypatch.setattr(
        module,
        "load_persisted_inputs",
        lambda _client, *, strategy, horizon, universe: persisted,
    )
    monkeypatch.setattr(module, "_git_sha", lambda: "a" * 40)
    monkeypatch.setattr(module, "_connect_metadata", lambda _database_url: connection)
    monkeypatch.setattr(module, "BacktestMetadataRepository", lambda _conn: repo)

    if isinstance(falsifier_result, BaseException):

        def raise_error(*_args: Any, **_kwargs: Any) -> None:
            raise falsifier_result

        monkeypatch.setattr(module, "run_momentum_falsifier", raise_error)
    else:
        monkeypatch.setattr(
            module,
            "run_momentum_falsifier",
            lambda *_args, **_kwargs: falsifier_result,
        )


class RecordingConnection:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


class RecordingMetadataRepository:
    def __init__(self) -> None:
        self.model_create: Any | None = None
        self.model_finish: Any | None = None
        self.backtest_create: Any | None = None
        self.backtest_finish: Any | None = None

    def create_model_run(self, run: Any) -> Any:
        self.model_create = run
        return SimpleNamespace(
            id=101,
            model_run_key=run.model_run_key,
            status="running",
        )

    def finish_model_run(self, run_id: int, finish: Any) -> Any:
        self.model_finish = finish
        assert self.model_create is not None
        return SimpleNamespace(
            id=run_id,
            model_run_key=self.model_create.model_run_key,
            status=finish.status,
        )

    def create_backtest_run(self, run: Any) -> Any:
        self.backtest_create = run
        return SimpleNamespace(
            id=202,
            backtest_run_key=run.backtest_run_key,
            status="running",
        )

    def finish_backtest_run(self, run_id: int, finish: Any) -> Any:
        self.backtest_finish = finish
        assert self.backtest_create is not None
        return SimpleNamespace(
            id=run_id,
            backtest_run_key=self.backtest_create.backtest_run_key,
            status=finish.status,
        )
