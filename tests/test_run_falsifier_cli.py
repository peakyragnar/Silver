from __future__ import annotations

import importlib.util
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUN_FALSIFIER_SCRIPT = ROOT / "scripts" / "run_falsifier.py"
CLI_SPEC = importlib.util.spec_from_file_location("run_falsifier_cli", RUN_FALSIFIER_SCRIPT)
assert CLI_SPEC is not None
assert CLI_SPEC.loader is not None
cli = importlib.util.module_from_spec(CLI_SPEC)
sys.modules[CLI_SPEC.name] = cli
CLI_SPEC.loader.exec_module(cli)


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


def test_report_run_creates_and_finishes_model_run_success(
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
    monkeypatch.setattr(cli, "_load_run_identity", lambda *args, **kwargs: None)
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
    assert len(repo.created) == 1
    assert len(repo.finishes) == 1
    created = repo.created[0]
    assert created.name == "Momentum 12-1 falsifier"
    assert created.code_git_sha == "abcdef0"
    assert created.feature_set_hash == cli._feature_set_hash(feature)
    assert created.random_seed == 0
    assert created.target_kind == "excess_return_market"
    assert created.training_start_date < created.training_end_date
    assert created.test_start_date > created.training_end_date
    assert created.test_end_date >= created.test_start_date
    assert created.available_at_policy_versions == {"daily_price": 1}
    assert created.input_fingerprints["row_count"] == len(rows)
    assert "joined_feature_label_rows_sha256" in created.input_fingerprints
    assert created.parameters["feature_definition"]["definition_hash"] == "a" * 64
    assert created.parameters["window_source"] == "scorable_walk_forward"
    assert created.cost_assumptions["round_trip_cost_bps"] == 20.0

    _run_id, finish = repo.finishes[0]
    assert finish.status == "succeeded"
    assert finish.metrics["split_count"] > 0
    assert finish.metrics["status"] == "succeeded"
    assert (tmp_path / "report.md").exists()


def test_report_run_finishes_model_run_as_insufficient_data(
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
    monkeypatch.setattr(cli, "_load_run_identity", lambda *args, **kwargs: None)
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
    assert repo.finishes[0][1].status == "insufficient_data"
    assert repo.finishes[0][1].metrics["split_count"] == 0
    assert repo.finishes[0][1].metrics["failure_modes"]
    assert repo.created[0].parameters["window_source"] == "input_coverage_fallback"


def test_report_run_finishes_model_run_as_failed_on_execution_error(
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

    assert repo.finishes[0][1].status == "failed"
    assert repo.finishes[0][1].metrics == {
        "error_message": "synthetic execution failure",
        "error_type": "MomentumFalsifierInputError",
    }
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


class FakeMetadataRepository:
    def __init__(self) -> None:
        self.created: list[Any] = []
        self.finishes: list[tuple[int, Any]] = []

    def create_model_run(self, run: Any) -> Any:
        self.created.append(run)
        return cli.ModelRunRecord(
            id=1,
            model_run_key=run.model_run_key,
            status="running",
        )

    def finish_model_run(self, run_id: int, finish: Any) -> Any:
        self.finishes.append((run_id, finish))
        return cli.ModelRunRecord(
            id=run_id,
            model_run_key=self.created[0].model_run_key,
            status=finish.status,
        )
