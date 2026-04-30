#!/usr/bin/env python
"""Run the Phase 1 falsifier and write the Week 1 momentum report."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from silver.analytics import (  # noqa: E402
    AnalyticsRunRecord,
    AnalyticsRunRepository,
    BacktestMetadataRepository,
    BacktestRunCreate,
    BacktestRunFinish,
    BacktestRunRecord,
    BacktestTraceabilitySnapshot,
    ModelRunCreate,
    ModelRunFinish,
    ModelRunRecord,
)
from silver.backtest.label_scramble import (  # noqa: E402
    LabelScrambleInputError,
    LabelScrambleSample,
    run_label_scramble,
)
from silver.backtest.momentum_falsifier import (  # noqa: E402
    DEFAULT_MIN_TRAIN_SESSIONS,
    DEFAULT_ROUND_TRIP_COST_BPS,
    DEFAULT_STEP_SESSIONS,
    DEFAULT_TEST_SESSIONS,
    MomentumBacktestRow,
    MomentumDateResult,
    MomentumFalsifierResult,
    MomentumFalsifierInputError,
    run_momentum_falsifier,
)
from silver.backtest.regimes import summarize_by_regime  # noqa: E402
from silver.backtest.walk_forward import (  # noqa: E402
    WalkForwardConfig,
    WalkForwardSplit,
    plan_walk_forward_splits,
)
from silver.features.momentum_12_1 import MOMENTUM_12_1_DEFINITION  # noqa: E402
from silver.reference.seed_data import (  # noqa: E402
    DEFAULT_CONFIG_PATH as DEFAULT_REFERENCE_CONFIG_PATH,
)
from silver.reference.seed_data import FALSIFIER_UNIVERSE_NAME, load_seed_file  # noqa: E402
from silver.reports.falsifier import (  # noqa: E402
    FalsifierDataCoverage,
    FalsifierEvidence,
    FalsifierFeatureMetadata,
    FalsifierInputCounts,
    FalsifierModelWindow,
    FalsifierReport,
    FalsifierReproducibilityMetadata,
    FalsifierRunIdentity,
    UniverseMember,
    coverage_from_rows,
    fingerprint_momentum_inputs,
    missing_prerequisite_message,
    render_week_1_momentum_report,
)
from silver.time.trading_calendar import (  # noqa: E402
    CANONICAL_HORIZONS,
    DEFAULT_SEED_PATH as DEFAULT_TRADING_CALENDAR_SEED_PATH,
)
from silver.time.trading_calendar import TradingCalendar, load_seed_csv  # noqa: E402


DEFAULT_OUTPUT_PATH = ROOT / "reports" / "falsifier" / "week_1_momentum.md"
TARGET_STRATEGY = MOMENTUM_12_1_DEFINITION.name
FALSIFIER_MODEL_RUN_NAME = "Momentum 12-1 falsifier"
FALSIFIER_INVOCATION_RUN_KIND = "falsifier_report_invocation"
FALSIFIER_RANDOM_SEED = 0
TARGET_COMMAND_TEMPLATE = (
    "python scripts/run_falsifier.py --strategy {strategy} --horizon {horizon} "
    "--universe {universe}"
)
DEFAULT_LABEL_SCRAMBLE_SEED = 44
DEFAULT_LABEL_SCRAMBLE_TRIAL_COUNT = 100
LABEL_SCRAMBLE_ALPHA = 0.05
MULTIPLE_COMPARISONS_CORRECTION = "none"


class FalsifierCliError(RuntimeError):
    """Raised when the falsifier CLI cannot complete."""


@dataclass(frozen=True, slots=True)
class FeatureDefinitionRecord:
    id: int
    name: str
    version: int
    definition_hash: str


@dataclass(frozen=True, slots=True)
class PersistedFalsifierInputs:
    universe_members: tuple[UniverseMember, ...]
    feature_definition: FeatureDefinitionRecord
    rows: tuple[MomentumBacktestRow, ...]
    available_at_policy_versions: Mapping[str, int]
    target_kind: str


@dataclass(frozen=True, slots=True)
class ModelRunWindow:
    training_start_date: date
    training_end_date: date
    test_start_date: date
    test_end_date: date
    source: str


@dataclass(frozen=True, slots=True)
class FalsifierReportRun:
    model_run: ModelRunRecord
    backtest_run: BacktestRunRecord
    status: str


class PsqlJsonClient:
    """Tiny psql-backed JSON reader for persisted Silver inputs."""

    def __init__(self, *, database_url: str, psql_path: str | None = None) -> None:
        self._database_url = database_url
        self._psql_path = psql_path or shutil.which("psql")
        if self._psql_path is None:
            raise FalsifierCliError("psql is required to read persisted falsifier inputs")

    def fetch_json(self, sql: str) -> Any:
        result = subprocess.run(
            [
                self._psql_path,
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-q",
                "-t",
                "-A",
                "-d",
                self._database_url,
            ],
            input=sql,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            stderr = result.stderr.replace(self._database_url, "[DATABASE_URL]").strip()
            detail = f": {stderr}" if stderr else ""
            raise FalsifierCliError(
                "psql failed while reading persisted falsifier inputs"
                f"{detail}. If the schema is missing, run "
                "`python scripts/bootstrap_database.py` first."
            )
        output = result.stdout.strip()
        if not output:
            raise FalsifierCliError("psql returned no JSON output")
        try:
            return json.loads(output)
        except json.JSONDecodeError as exc:
            raise FalsifierCliError("psql returned invalid JSON") from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--strategy",
        default=TARGET_STRATEGY,
        choices=(TARGET_STRATEGY,),
        help="falsifier strategy to run",
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=63,
        help="forward-return horizon in trading sessions",
    )
    parser.add_argument(
        "--universe",
        default=FALSIFIER_UNIVERSE_NAME,
        help="point-in-time universe name",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="markdown report path",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate CLI/config/report path without live data",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL; defaults to DATABASE_URL",
    )
    parser.add_argument(
        "--psql-path",
        help="path to psql; defaults to the first psql on PATH",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        _validate_args(args)
        if args.check:
            run_check(args)
            return 0
        run_report(args)
    except (
        FalsifierCliError,
        MomentumFalsifierInputError,
        ValueError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


def run_check(args: argparse.Namespace) -> None:
    """Validate offline CLI/config/report-path prerequisites."""

    seed_config = load_seed_file(DEFAULT_REFERENCE_CONFIG_PATH)
    if args.universe not in {
        membership.universe_name for membership in seed_config.universe_memberships
    }:
        raise FalsifierCliError(
            f"universe `{args.universe}` is not present in "
            f"{DEFAULT_REFERENCE_CONFIG_PATH.relative_to(ROOT)}"
        )

    calendar_rows = load_seed_csv(DEFAULT_TRADING_CALENDAR_SEED_PATH)
    TradingCalendar(calendar_rows)
    _validate_report_path(args.output_path)
    print(
        "OK: falsifier CLI check passed for "
        f"{_target_command(args)} -> {_display_path(args.output_path)}"
    )


def run_report(args: argparse.Namespace) -> None:
    if not args.database_url:
        raise FalsifierCliError(
            "DATABASE_URL is required unless --check is used. Run "
            "`python scripts/bootstrap_database.py` after setting DATABASE_URL, "
            "then rerun the falsifier command."
        )

    calendar = TradingCalendar(load_seed_csv(DEFAULT_TRADING_CALENDAR_SEED_PATH))
    client = PsqlJsonClient(database_url=args.database_url, psql_path=args.psql_path)
    connection = _connect_metadata_database(args.database_url)
    try:
        try:
            report_run = run_report_with_metadata(
                args,
                client=client,
                metadata_repository=BacktestMetadataRepository(connection),
                invocation_repository=AnalyticsRunRepository(connection),
                calendar=calendar,
            )
        except Exception:
            _commit(connection)
            raise
        else:
            _commit(connection)
    finally:
        _close(connection)
    print(
        f"OK: wrote {_display_path(args.output_path)} with status "
        f"{report_run.status}; model_run_id={report_run.model_run.id}; "
        f"backtest_run_id={report_run.backtest_run.id}"
    )


def run_report_with_metadata(
    args: argparse.Namespace,
    *,
    client: PsqlJsonClient,
    metadata_repository: BacktestMetadataRepository,
    calendar: TradingCalendar,
    invocation_repository: AnalyticsRunRepository | None = None,
) -> FalsifierReportRun:
    persisted_inputs = load_persisted_inputs(
        client,
        strategy=args.strategy,
        horizon=args.horizon,
        universe=args.universe,
    )
    feature = persisted_inputs.feature_definition
    feature_set_hash = _feature_set_hash(feature)
    git_sha = _git_sha()
    input_fingerprint = fingerprint_momentum_inputs(persisted_inputs.rows)
    data_coverage = coverage_from_rows(persisted_inputs.rows)
    model_window = _model_run_window(
        persisted_inputs.rows,
        calendar=calendar,
        horizon=args.horizon,
    )
    model_run = metadata_repository.create_model_run(
        _model_run_create(
            args,
            persisted_inputs=persisted_inputs,
            feature_set_hash=feature_set_hash,
            git_sha=git_sha,
            input_fingerprint=input_fingerprint,
            data_coverage=data_coverage,
            window=model_window,
        )
    )
    backtest_run = metadata_repository.create_backtest_run(
        _backtest_run_create(
            args,
            model_run=model_run,
            persisted_inputs=persisted_inputs,
        )
    )
    invocation_run = _create_invocation_run(
        invocation_repository,
        args,
        git_sha=git_sha,
        input_fingerprint=input_fingerprint,
        model_run=model_run,
        backtest_run=backtest_run,
        persisted_inputs=persisted_inputs,
    )

    try:
        result = run_momentum_falsifier(
            persisted_inputs.rows,
            calendar=calendar,
            horizon_sessions=args.horizon,
            min_train_sessions=DEFAULT_MIN_TRAIN_SESSIONS,
            test_sessions=DEFAULT_TEST_SESSIONS,
            step_sessions=DEFAULT_STEP_SESSIONS,
            round_trip_cost_bps=DEFAULT_ROUND_TRIP_COST_BPS,
        )
    except Exception as exc:
        _finish_failed_metadata(
            metadata_repository,
            model_run=model_run,
            backtest_run=backtest_run,
            error=exc,
        )
        _finish_invocation_run(
            invocation_repository,
            invocation_run,
            status="failed",
        )
        raise

    model_finish = ModelRunFinish(
        status=result.status,
        metrics=_model_run_metrics(result),
    )
    backtest_finish = _backtest_run_finish(
        result=result,
        rows=persisted_inputs.rows,
        failure_message=None,
    )
    report = FalsifierReport(
        strategy=args.strategy,
        horizon=args.horizon,
        universe_name=args.universe,
        universe_members=persisted_inputs.universe_members,
        data_coverage=data_coverage,
        feature_metadata=FalsifierFeatureMetadata(
            name=feature.name,
            version=feature.version,
            definition_hash=feature.definition_hash,
            feature_set_hash=feature_set_hash,
        ),
        backtest_result=result,
        reproducibility=FalsifierReproducibilityMetadata(
            command=_target_command(args),
            git_sha=git_sha,
            input_fingerprint=input_fingerprint,
            available_at_policy_versions=persisted_inputs.available_at_policy_versions,
            run_identity=FalsifierRunIdentity(
                model_run_id=model_run.id,
                model_run_key=model_run.model_run_key,
                backtest_run_id=backtest_run.id,
                backtest_run_key=backtest_run.backtest_run_key,
            ),
            model_window=FalsifierModelWindow(
                training_start_date=model_window.training_start_date,
                training_end_date=model_window.training_end_date,
                test_start_date=model_window.test_start_date,
                test_end_date=model_window.test_end_date,
                source=model_window.source,
            ),
            target_kind=persisted_inputs.target_kind,
            random_seed=FALSIFIER_RANDOM_SEED,
            execution_assumptions=_execution_assumptions(),
        ),
        evidence=_report_evidence(backtest_finish),
    )
    finished_model = _finish_or_reuse_model_run(
        metadata_repository,
        model_run=model_run,
        finish=model_finish,
    )
    finished_backtest = _finish_or_reuse_backtest_run(
        metadata_repository,
        backtest_run=backtest_run,
        finish=backtest_finish,
    )
    validate_falsifier_report_traceability(
        report,
        metadata_repository,
        expected_backtest_finish=backtest_finish,
    )
    write_report(args.output_path, render_week_1_momentum_report(report))
    _finish_invocation_run(
        invocation_repository,
        invocation_run,
        status="succeeded",
    )
    return FalsifierReportRun(
        model_run=finished_model,
        backtest_run=finished_backtest,
        status=result.status,
    )


def load_persisted_inputs(
    client: PsqlJsonClient,
    *,
    strategy: str,
    horizon: int,
    universe: str,
) -> PersistedFalsifierInputs:
    universe_members = _load_universe_members(client, universe)
    feature_definition = _load_feature_definition(client, strategy)
    counts = _load_input_counts(
        client,
        feature_definition_id=feature_definition.id,
        horizon=horizon,
        universe=universe,
    )
    missing_message = missing_prerequisite_message(
        counts,
        strategy=strategy,
        horizon=horizon,
        universe=universe,
    )
    if missing_message is not None:
        raise FalsifierCliError(missing_message)

    target_kind = _load_target_kind(
        client,
        feature_definition_id=feature_definition.id,
        horizon=horizon,
        universe=universe,
    )
    rows = _load_backtest_rows(
        client,
        feature_definition_id=feature_definition.id,
        horizon=horizon,
        universe=universe,
    )
    return PersistedFalsifierInputs(
        universe_members=universe_members,
        feature_definition=feature_definition,
        rows=rows,
        available_at_policy_versions=_load_policy_versions(
            client,
            feature_definition_id=feature_definition.id,
            horizon=horizon,
            universe=universe,
        ),
        target_kind=target_kind,
    )


def write_report(path: Path, content: str) -> None:
    report_path = _resolve_repo_path(path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(content, encoding="utf-8")


def _backtest_run_create(
    args: argparse.Namespace,
    *,
    model_run: ModelRunRecord,
    persisted_inputs: PersistedFalsifierInputs,
) -> BacktestRunCreate:
    return BacktestRunCreate(
        backtest_run_key=_backtest_run_key(args, model_run.model_run_key),
        model_run_id=model_run.id,
        name=f"{args.strategy} falsifier backtest",
        universe_name=args.universe,
        horizon_days=args.horizon,
        target_kind=persisted_inputs.target_kind,
        cost_assumptions=_cost_assumptions(None),
        parameters={
            "command": _target_command(args),
            "feature_definition": _feature_definition_parameters(
                persisted_inputs.feature_definition,
            ),
            "label_scramble_alpha": LABEL_SCRAMBLE_ALPHA,
            "label_scramble_seed": DEFAULT_LABEL_SCRAMBLE_SEED,
            "label_scramble_trial_count": DEFAULT_LABEL_SCRAMBLE_TRIAL_COUNT,
            "metadata_role": "backtest_run",
            "min_train_sessions": DEFAULT_MIN_TRAIN_SESSIONS,
            "model_run_key": model_run.model_run_key,
            "multiple_comparisons_correction": MULTIPLE_COMPARISONS_CORRECTION,
            "step_sessions": DEFAULT_STEP_SESSIONS,
            "strategy": args.strategy,
            "target_kind": persisted_inputs.target_kind,
            "test_sessions": DEFAULT_TEST_SESSIONS,
            "universe": args.universe,
        },
        multiple_comparisons_correction=MULTIPLE_COMPARISONS_CORRECTION,
    )


def _backtest_run_key(args: argparse.Namespace, model_run_key: str) -> str:
    payload = {
        "contract": "falsifier-backtest-run-identity",
        "cost_assumptions": _cost_assumptions(None),
        "horizon": args.horizon,
        "label_scramble": {
            "alpha": LABEL_SCRAMBLE_ALPHA,
            "seed": DEFAULT_LABEL_SCRAMBLE_SEED,
            "trial_count": DEFAULT_LABEL_SCRAMBLE_TRIAL_COUNT,
        },
        "model_run_key": model_run_key,
        "multiple_comparisons_correction": MULTIPLE_COMPARISONS_CORRECTION,
        "strategy": args.strategy,
        "universe": args.universe,
        "walk_forward": {
            "min_train_sessions": DEFAULT_MIN_TRAIN_SESSIONS,
            "step_sessions": DEFAULT_STEP_SESSIONS,
            "test_sessions": DEFAULT_TEST_SESSIONS,
        },
        "version": 2,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"falsifier-backtest-{args.strategy}-h{args.horizon}-{digest[:16]}"


def _backtest_run_finish(
    *,
    result: MomentumFalsifierResult | None,
    rows: Sequence[MomentumBacktestRow],
    failure_message: str | None,
) -> BacktestRunFinish:
    label_scramble_metrics, label_scramble_pass = _label_scramble_payload(
        rows=rows,
        result=result,
    )
    status = "failed" if result is None else result.status
    return BacktestRunFinish(
        status=status,
        cost_assumptions=_cost_assumptions(result),
        metrics=_metrics_payload(result=result, failure_message=failure_message),
        metrics_by_regime=_metrics_by_regime(result),
        baseline_metrics=_baseline_metrics(result),
        label_scramble_metrics=label_scramble_metrics,
        label_scramble_pass=label_scramble_pass,
        multiple_comparisons_correction=MULTIPLE_COMPARISONS_CORRECTION,
    )


def _report_evidence(backtest_finish: BacktestRunFinish) -> FalsifierEvidence:
    return FalsifierEvidence(
        metrics_by_regime=backtest_finish.metrics_by_regime,
        label_scramble_metrics=backtest_finish.label_scramble_metrics,
        label_scramble_pass=backtest_finish.label_scramble_pass,
        multiple_comparisons_correction=(
            backtest_finish.multiple_comparisons_correction
        ),
    )


def _finish_failed_metadata(
    metadata_repository: BacktestMetadataRepository,
    *,
    model_run: ModelRunRecord,
    backtest_run: BacktestRunRecord,
    error: Exception,
) -> None:
    if model_run.status == "running":
        metadata_repository.finish_model_run(
            model_run.id,
            ModelRunFinish(
                status="failed",
                metrics={
                    "error_message": str(error),
                    "error_type": type(error).__name__,
                },
            ),
        )
    if backtest_run.status == "running":
        metadata_repository.finish_backtest_run(
            backtest_run.id,
            _backtest_run_finish(
                result=None,
                rows=(),
                failure_message=str(error),
            ),
        )


def _create_invocation_run(
    invocation_repository: AnalyticsRunRepository | None,
    args: argparse.Namespace,
    *,
    git_sha: str,
    input_fingerprint: str,
    model_run: ModelRunRecord,
    backtest_run: BacktestRunRecord,
    persisted_inputs: PersistedFalsifierInputs,
) -> AnalyticsRunRecord | None:
    if invocation_repository is None:
        return None
    return invocation_repository.create_run(
        run_kind=FALSIFIER_INVOCATION_RUN_KIND,
        code_git_sha=git_sha,
        available_at_policy_versions=persisted_inputs.available_at_policy_versions,
        parameters=_invocation_parameters(
            args,
            model_run=model_run,
            backtest_run=backtest_run,
        ),
        input_fingerprints={
            "joined_feature_label_rows_sha256": input_fingerprint,
        },
        random_seed=FALSIFIER_RANDOM_SEED,
    )


def _invocation_parameters(
    args: argparse.Namespace,
    *,
    model_run: ModelRunRecord,
    backtest_run: BacktestRunRecord,
) -> dict[str, object]:
    return {
        "backtest_run_id": backtest_run.id,
        "backtest_run_key": backtest_run.backtest_run_key,
        "command": _target_command(args),
        "horizon": args.horizon,
        "invocation_id": str(uuid.uuid4()),
        "metadata_role": "falsifier_invocation",
        "model_run_id": model_run.id,
        "model_run_key": model_run.model_run_key,
        "output_path": _display_path(args.output_path),
        "process_id": os.getpid(),
        "strategy": args.strategy,
        "universe": args.universe,
    }


def _finish_invocation_run(
    invocation_repository: AnalyticsRunRepository | None,
    invocation_run: AnalyticsRunRecord | None,
    *,
    status: str,
) -> AnalyticsRunRecord | None:
    if invocation_repository is None or invocation_run is None:
        return None
    return invocation_repository.finish_run(invocation_run.id, status=status)


def _finish_or_reuse_model_run(
    metadata_repository: BacktestMetadataRepository,
    *,
    model_run: ModelRunRecord,
    finish: ModelRunFinish,
) -> ModelRunRecord:
    if model_run.status == "running":
        return metadata_repository.finish_model_run(model_run.id, finish)
    if model_run.status != finish.status:
        raise FalsifierCliError(
            f"model_run_key {model_run.model_run_key} already has status "
            f"{model_run.status}; rerun produced {finish.status}"
        )
    return model_run


def _finish_or_reuse_backtest_run(
    metadata_repository: BacktestMetadataRepository,
    *,
    backtest_run: BacktestRunRecord,
    finish: BacktestRunFinish,
) -> BacktestRunRecord:
    if backtest_run.status == "running":
        return metadata_repository.finish_backtest_run(backtest_run.id, finish)
    if backtest_run.status != finish.status:
        raise FalsifierCliError(
            f"backtest_run_key {backtest_run.backtest_run_key} already has status "
            f"{backtest_run.status}; rerun produced {finish.status}"
        )
    return backtest_run


def _cost_assumptions(result: MomentumFalsifierResult | None) -> dict[str, object]:
    round_trip_cost_bps = (
        DEFAULT_ROUND_TRIP_COST_BPS
        if result is None
        else result.round_trip_cost_bps
    )
    return {
        "round_trip_cost_bps": round_trip_cost_bps,
        "application": (
            "Subtracted from strategy and equal-weight baseline returns for "
            "each scored test date."
        ),
    }


def _model_run_cost_assumptions() -> dict[str, object]:
    return {
        "application": (
            "subtracted from strategy and equal-weight baseline returns "
            "for each scored test date"
        ),
        "round_trip_cost_bps": DEFAULT_ROUND_TRIP_COST_BPS,
    }


def _execution_assumptions() -> dict[str, object]:
    return {
        "label_scramble_alpha": LABEL_SCRAMBLE_ALPHA,
        "label_scramble_seed": DEFAULT_LABEL_SCRAMBLE_SEED,
        "label_scramble_trial_count": DEFAULT_LABEL_SCRAMBLE_TRIAL_COUNT,
        "min_train_sessions": DEFAULT_MIN_TRAIN_SESSIONS,
        "multiple_comparisons_correction": MULTIPLE_COMPARISONS_CORRECTION,
        "round_trip_cost_bps": DEFAULT_ROUND_TRIP_COST_BPS,
        "step_sessions": DEFAULT_STEP_SESSIONS,
        "test_sessions": DEFAULT_TEST_SESSIONS,
    }


def _metrics_payload(
    *,
    result: MomentumFalsifierResult | None,
    failure_message: str | None,
) -> dict[str, object]:
    if result is None:
        return {
            "status": "failed",
            "failure_message": failure_message,
        }

    metrics = result.headline_metrics
    return {
        "status": result.status,
        "failure_modes": list(result.failure_modes),
        "scored_walk_forward_windows": metrics.split_count,
        "scored_test_dates": metrics.scored_test_dates,
        "eligible_observations": metrics.eligible_observations,
        "selected_observations": metrics.selected_observations,
        "mean_strategy_gross_horizon_return": (
            metrics.mean_strategy_gross_return
        ),
        "mean_strategy_net_horizon_return": metrics.mean_strategy_net_return,
        "strategy_net_hit_rate": metrics.strategy_net_hit_rate,
        "strategy_net_return_stddev": metrics.strategy_net_return_stddev,
        "strategy_net_return_to_stddev": metrics.strategy_net_return_to_stddev,
    }


def _baseline_metrics(
    result: MomentumFalsifierResult | None,
) -> dict[str, object]:
    if result is None:
        return {
            "status": "not_available",
            "reason": "falsifier execution failed before baseline metrics existed",
        }

    metrics = result.headline_metrics
    return {
        "equal_weight_universe": {
            "mean_gross_horizon_return": metrics.mean_baseline_gross_return,
            "mean_net_horizon_return": metrics.mean_baseline_net_return,
        },
        "strategy_vs_equal_weight_universe": {
            "mean_net_difference": metrics.mean_net_difference_vs_baseline,
        },
    }


def _metrics_by_regime(
    result: MomentumFalsifierResult | None,
) -> dict[str, object]:
    if result is None:
        return {
            "status": "not_available",
            "reason": "falsifier execution failed before regime metrics existed",
        }

    date_results = _date_results(result)
    strategy_summaries = summarize_by_regime(
        date_results,
        date_getter=lambda row: row.asof_date,
        value_getter=lambda row: row.strategy_net_return,
    )
    baseline_summaries = summarize_by_regime(
        date_results,
        date_getter=lambda row: row.asof_date,
        value_getter=lambda row: row.baseline_net_return,
    )
    diff_summaries = summarize_by_regime(
        date_results,
        date_getter=lambda row: row.asof_date,
        value_getter=lambda row: row.strategy_net_return - row.baseline_net_return,
    )
    baseline_by_name = {summary.regime_name: summary for summary in baseline_summaries}
    diff_by_name = {summary.regime_name: summary for summary in diff_summaries}
    return {
        summary.regime_name: {
            "start_date": summary.start_date.isoformat(),
            "end_date": summary.end_date.isoformat(),
            "sample_count": summary.sample_count,
            "strategy_net_return": _regime_summary(summary),
            "baseline_net_return": _regime_summary(
                baseline_by_name[summary.regime_name]
            ),
            "net_difference_vs_baseline": _regime_summary(
                diff_by_name[summary.regime_name]
            ),
        }
        for summary in strategy_summaries
    }


def _regime_summary(summary: object) -> dict[str, object]:
    return {
        "value_count": summary.value_count,
        "mean": summary.mean,
        "sample_stddev": summary.sample_stddev,
        "minimum": summary.minimum,
        "maximum": summary.maximum,
        "hit_rate": summary.hit_rate,
    }


def _label_scramble_payload(
    *,
    rows: Sequence[MomentumBacktestRow],
    result: MomentumFalsifierResult | None,
) -> tuple[dict[str, object], bool]:
    if result is None:
        return (
            {
                "status": "not_run",
                "reason": "falsifier execution failed before label scramble",
            },
            False,
        )
    if result.headline_metrics.scored_test_dates == 0:
        return (
            {
                "status": "not_run",
                "reason": "no scored walk-forward test rows were available",
                "scored_row_source": "reported_scored_walk_forward_test_rows",
                "selection_rule": "reported_top_half_selection_mask",
                "seed": DEFAULT_LABEL_SCRAMBLE_SEED,
                "trial_count": DEFAULT_LABEL_SCRAMBLE_TRIAL_COUNT,
            },
            False,
        )
    try:
        cost_fraction = result.round_trip_cost_bps / 10_000.0
        scramble_result = run_label_scramble(
            _label_scramble_samples(rows, result),
            seed=DEFAULT_LABEL_SCRAMBLE_SEED,
            trial_count=DEFAULT_LABEL_SCRAMBLE_TRIAL_COUNT,
            scoring_function=_selected_group_mean_net_return_scorer(cost_fraction),
        )
    except LabelScrambleInputError as exc:
        return (
            {
                "status": "not_run",
                "reason": str(exc),
                "scored_row_source": "reported_scored_walk_forward_test_rows",
                "selection_rule": "reported_top_half_selection_mask",
                "seed": DEFAULT_LABEL_SCRAMBLE_SEED,
                "trial_count": DEFAULT_LABEL_SCRAMBLE_TRIAL_COUNT,
            },
            False,
        )

    payload = scramble_result.to_dict()
    payload["status"] = "completed"
    payload["alpha"] = LABEL_SCRAMBLE_ALPHA
    payload["scored_row_source"] = "reported_scored_walk_forward_test_rows"
    payload["selection_rule"] = "reported_top_half_selection_mask"
    payload["eligible_observations"] = result.headline_metrics.eligible_observations
    payload["selected_observations"] = result.headline_metrics.selected_observations
    payload["scored_test_dates"] = result.headline_metrics.scored_test_dates
    payload["reported_mean_strategy_net_horizon_return"] = (
        result.headline_metrics.mean_strategy_net_return
    )
    payload["sample_feature_value"] = (
        "1.0 when the ticker is selected by the reported strategy path; "
        "0.0 otherwise"
    )
    return (
        payload,
        result.status == "succeeded" and scramble_result.p_value <= LABEL_SCRAMBLE_ALPHA,
    )


def _label_scramble_samples(
    rows: Sequence[MomentumBacktestRow],
    result: MomentumFalsifierResult,
) -> tuple[LabelScrambleSample, ...]:
    rows_by_date: dict[date, list[MomentumBacktestRow]] = {}
    for row in rows:
        rows_by_date.setdefault(row.asof_date, []).append(row)

    samples: list[LabelScrambleSample] = []
    for date_result in _date_results(result):
        selected_tickers = set(date_result.selected_tickers)
        if len(selected_tickers) != len(date_result.selected_tickers):
            raise LabelScrambleInputError(
                "reported strategy selection contains duplicate tickers for "
                f"{date_result.asof_date.isoformat()}"
            )
        scored_rows = tuple(
            sorted(
                rows_by_date.get(date_result.asof_date, ()),
                key=lambda row: row.ticker,
            )
        )
        if not scored_rows:
            raise LabelScrambleInputError(
                "reported scored test date has no matching feature/label rows: "
                f"{date_result.asof_date.isoformat()}"
            )
        if len(scored_rows) != date_result.eligible_count:
            raise LabelScrambleInputError(
                "reported scored test date row count does not match "
                "eligible_count for "
                f"{date_result.asof_date.isoformat()}"
            )
        scored_tickers = {row.ticker for row in scored_rows}
        missing_tickers = sorted(selected_tickers - scored_tickers)
        if missing_tickers:
            raise LabelScrambleInputError(
                "reported selected tickers are missing from scored rows for "
                f"{date_result.asof_date.isoformat()}: "
                f"{', '.join(missing_tickers)}"
            )

        group_key = date_result.asof_date.isoformat()
        for row in scored_rows:
            samples.append(
                LabelScrambleSample(
                    sample_id=f"{row.ticker}-{group_key}",
                    feature_value=1.0 if row.ticker in selected_tickers else 0.0,
                    label_value=row.realized_return,
                    group_key=group_key,
                )
            )

    return tuple(samples)


def _selected_group_mean_net_return_scorer(
    cost_fraction: float,
) -> Callable[[tuple[LabelScrambleSample, ...]], float]:
    def selected_group_mean_net_return(
        samples: tuple[LabelScrambleSample, ...],
    ) -> float:
        groups: dict[str, list[LabelScrambleSample]] = {}
        for sample in samples:
            groups.setdefault(sample.group_key, []).append(sample)
        if not groups:
            raise LabelScrambleInputError("label scramble requires scored test rows")

        group_returns: list[float] = []
        for group_key in sorted(groups):
            selected_labels = [
                sample.label_value
                for sample in groups[group_key]
                if sample.feature_value == 1.0
            ]
            if not selected_labels:
                raise LabelScrambleInputError(
                    "label scramble selection mask has no selected rows for "
                    f"{group_key}"
                )
            group_returns.append(
                sum(selected_labels) / len(selected_labels) - cost_fraction
            )
        return sum(group_returns) / len(group_returns)

    return selected_group_mean_net_return


def _date_results(result: MomentumFalsifierResult) -> tuple[MomentumDateResult, ...]:
    return tuple(
        date_result
        for window in result.windows
        for date_result in window.date_results
    )


def _load_universe_members(
    client: PsqlJsonClient,
    universe: str,
) -> tuple[UniverseMember, ...]:
    rows = client.fetch_json(
        f"""
SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY ticker), '[]'::jsonb)::text
FROM (
    SELECT
        s.ticker,
        um.valid_from::text AS valid_from,
        um.valid_to::text AS valid_to
    FROM silver.universe_membership um
    JOIN silver.securities s ON s.id = um.security_id
    WHERE um.universe_name = {_sql_literal(universe)}
    ORDER BY s.ticker, um.valid_from
) row;
""".strip()
    )
    return tuple(
        UniverseMember(
            ticker=_required_str(row, "ticker"),
            valid_from=date.fromisoformat(_required_str(row, "valid_from")),
            valid_to=_optional_date(row.get("valid_to")),
        )
        for row in rows
    )


def _load_feature_definition(
    client: PsqlJsonClient,
    strategy: str,
) -> FeatureDefinitionRecord:
    rows = client.fetch_json(
        f"""
SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY version DESC), '[]'::jsonb)::text
FROM (
    SELECT id, name, version, definition_hash
    FROM silver.feature_definitions
    WHERE name = {_sql_literal(strategy)}
    ORDER BY version DESC
    LIMIT 1
) row;
""".strip()
    )
    if not rows:
        raise FalsifierCliError(
            f"Missing prerequisite data: feature definition `{strategy}` is not "
            "persisted. Run the momentum feature materialization step after "
            "daily prices are normalized."
        )
    row = rows[0]
    return FeatureDefinitionRecord(
        id=_required_int(row, "id"),
        name=_required_str(row, "name"),
        version=_required_int(row, "version"),
        definition_hash=_required_str(row, "definition_hash"),
    )


def _load_input_counts(
    client: PsqlJsonClient,
    *,
    feature_definition_id: int,
    horizon: int,
    universe: str,
) -> FalsifierInputCounts:
    rows = client.fetch_json(
        f"""
WITH universe_rows AS (
    SELECT s.id AS security_id, um.valid_from, um.valid_to
    FROM silver.universe_membership um
    JOIN silver.securities s ON s.id = um.security_id
    WHERE um.universe_name = {_sql_literal(universe)}
),
feature_rows AS (
    SELECT fv.security_id, fv.asof_date
    FROM silver.feature_values fv
    JOIN universe_rows u ON u.security_id = fv.security_id
    WHERE fv.feature_definition_id = {feature_definition_id}
      AND fv.asof_date >= u.valid_from
      AND (u.valid_to IS NULL OR fv.asof_date <= u.valid_to)
),
label_rows AS (
    SELECT frl.security_id, frl.label_date
    FROM silver.forward_return_labels frl
    JOIN universe_rows u ON u.security_id = frl.security_id
    WHERE frl.horizon_days = {horizon}
      AND frl.label_date >= u.valid_from
      AND (u.valid_to IS NULL OR frl.label_date <= u.valid_to)
),
joined_rows AS (
    SELECT feature_rows.security_id, feature_rows.asof_date
    FROM feature_rows
    JOIN label_rows
      ON label_rows.security_id = feature_rows.security_id
     AND label_rows.label_date = feature_rows.asof_date
)
SELECT jsonb_build_object(
    'universe_members', (SELECT count(*) FROM universe_rows),
    'feature_values', (SELECT count(*) FROM feature_rows),
    'labels', (SELECT count(*) FROM label_rows),
    'joined_rows', (SELECT count(*) FROM joined_rows)
)::text;
""".strip()
    )
    return FalsifierInputCounts(
        universe_members=_required_int(rows, "universe_members"),
        feature_values=_required_int(rows, "feature_values"),
        labels=_required_int(rows, "labels"),
        joined_rows=_required_int(rows, "joined_rows"),
    )


def _load_backtest_rows(
    client: PsqlJsonClient,
    *,
    feature_definition_id: int,
    horizon: int,
    universe: str,
) -> tuple[MomentumBacktestRow, ...]:
    rows = client.fetch_json(
        f"""
SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY asof_date, ticker), '[]'::jsonb)::text
FROM (
    SELECT
        s.ticker,
        fv.asof_date::text AS asof_date,
        frl.horizon_date::text AS horizon_date,
        fv.value::float8 AS feature_value,
        COALESCE(
            frl.realized_excess_return,
            frl.realized_raw_return
        )::float8 AS realized_return
    FROM silver.feature_values fv
    JOIN silver.forward_return_labels frl
      ON frl.security_id = fv.security_id
     AND frl.label_date = fv.asof_date
     AND frl.horizon_days = {horizon}
    JOIN silver.securities s ON s.id = fv.security_id
    JOIN silver.universe_membership um
      ON um.security_id = fv.security_id
     AND um.universe_name = {_sql_literal(universe)}
     AND fv.asof_date >= um.valid_from
     AND (um.valid_to IS NULL OR fv.asof_date <= um.valid_to)
    WHERE fv.feature_definition_id = {feature_definition_id}
    ORDER BY fv.asof_date, s.ticker
) row;
""".strip()
    )
    return tuple(
        MomentumBacktestRow(
            ticker=_required_str(row, "ticker"),
            asof_date=date.fromisoformat(_required_str(row, "asof_date")),
            horizon_date=date.fromisoformat(_required_str(row, "horizon_date")),
            feature_value=float(row["feature_value"]),
            realized_return=float(row["realized_return"]),
        )
        for row in rows
    )


def _load_target_kind(
    client: PsqlJsonClient,
    *,
    feature_definition_id: int,
    horizon: int,
    universe: str,
) -> str:
    rows = client.fetch_json(
        f"""
WITH universe_rows AS (
    SELECT s.id AS security_id, um.valid_from, um.valid_to
    FROM silver.universe_membership um
    JOIN silver.securities s ON s.id = um.security_id
    WHERE um.universe_name = {_sql_literal(universe)}
),
joined_rows AS (
    SELECT frl.realized_excess_return
    FROM silver.feature_values fv
    JOIN silver.forward_return_labels frl
      ON frl.security_id = fv.security_id
     AND frl.label_date = fv.asof_date
     AND frl.horizon_days = {horizon}
    JOIN universe_rows u
      ON u.security_id = fv.security_id
     AND fv.asof_date >= u.valid_from
     AND (u.valid_to IS NULL OR fv.asof_date <= u.valid_to)
    WHERE fv.feature_definition_id = {feature_definition_id}
)
SELECT jsonb_build_object(
    'joined_rows', count(*),
    'excess_rows', count(*) FILTER (WHERE realized_excess_return IS NOT NULL)
)::text
FROM joined_rows;
""".strip()
    )
    if not isinstance(rows, Mapping):
        raise FalsifierCliError("target kind query returned non-object")
    joined_rows = _required_int(rows, "joined_rows")
    excess_rows = _required_int(rows, "excess_rows")
    if joined_rows <= 0:
        raise FalsifierCliError("target kind query returned no joined rows")
    if excess_rows == 0:
        return "raw_return"
    if excess_rows == joined_rows:
        return "excess_return_market"
    raise FalsifierCliError(
        "joined falsifier labels mix raw-return and excess-return targets; "
        "materialize one consistent target before writing model-run metadata"
    )


def _load_policy_versions(
    client: PsqlJsonClient,
    *,
    feature_definition_id: int,
    horizon: int,
    universe: str,
) -> Mapping[str, int]:
    normalized_feature_definition_id = _sql_positive_int(
        feature_definition_id,
        "feature_definition_id",
    )
    normalized_horizon = _sql_positive_int(horizon, "horizon")
    normalized_universe = _sql_required_str(universe, "universe")
    rows = client.fetch_json(
        f"""
WITH universe_rows AS (
    SELECT s.id AS security_id, um.valid_from, um.valid_to
    FROM silver.universe_membership um
    JOIN silver.securities s ON s.id = um.security_id
    WHERE um.universe_name = {_sql_literal(normalized_universe)}
),
joined_rows AS (
    SELECT
        fv.available_at_policy_id AS feature_available_at_policy_id,
        frl.available_at_policy_id AS label_available_at_policy_id
    FROM silver.feature_values fv
    JOIN silver.forward_return_labels frl
      ON frl.security_id = fv.security_id
     AND frl.label_date = fv.asof_date
     AND frl.horizon_days = {normalized_horizon}
    JOIN universe_rows u
      ON u.security_id = fv.security_id
     AND fv.asof_date >= u.valid_from
     AND (u.valid_to IS NULL OR fv.asof_date <= u.valid_to)
    WHERE fv.feature_definition_id = {normalized_feature_definition_id}
),
policy_ids AS (
    SELECT feature_available_at_policy_id AS available_at_policy_id
    FROM joined_rows
    UNION
    SELECT label_available_at_policy_id AS available_at_policy_id
    FROM joined_rows
),
policy_rows AS (
    SELECT DISTINCT policy.name, policy.version
    FROM silver.available_at_policies AS policy
    JOIN policy_ids
      ON policy_ids.available_at_policy_id = policy.id
)
SELECT jsonb_build_object(
    'policy_versions',
    COALESCE(
        (SELECT jsonb_object_agg(name, version ORDER BY name) FROM policy_rows),
        '{{}}'::jsonb
    ),
    'policy_name_count',
    (SELECT count(DISTINCT name) FROM policy_rows),
    'policy_pair_count',
    (SELECT count(*) FROM policy_rows)
)::text;
""".strip()
    )
    if not isinstance(rows, Mapping):
        raise FalsifierCliError("available_at policy versions query returned non-object")
    policy_versions = rows.get("policy_versions")
    if not isinstance(policy_versions, Mapping):
        raise FalsifierCliError(
            "available_at policy versions query returned missing policy_versions"
        )
    policy_name_count = _required_int(rows, "policy_name_count")
    policy_pair_count = _required_int(rows, "policy_pair_count")
    if policy_pair_count != policy_name_count:
        raise FalsifierCliError(
            "conflicting available_at policy versions found in joined falsifier "
            "input rows"
        )
    if not policy_versions:
        raise FalsifierCliError(
            "available_at policy versions query returned no joined input policies"
        )
    return {
        str(key): int(value)
        for key, value in sorted(policy_versions.items(), key=lambda item: str(item[0]))
    }


def _load_run_identity(
    client: PsqlJsonClient,
    *,
    horizon: int,
    universe: str,
    feature_set_hash: str,
    status: str,
) -> FalsifierRunIdentity | None:
    if not _registry_tables_exist(client):
        return None

    rows = client.fetch_json(
        f"""
SELECT COALESCE(jsonb_agg(to_jsonb(row)), '[]'::jsonb)::text
FROM (
    SELECT
        mr.id AS model_run_id,
        mr.model_run_key,
        br.id AS backtest_run_id,
        br.backtest_run_key
    FROM silver.backtest_runs br
    JOIN silver.model_runs mr ON mr.id = br.model_run_id
    WHERE br.universe_name = {_sql_literal(universe)}
      AND br.horizon_days = {horizon}
      AND br.status = {_sql_literal(status)}
      AND mr.horizon_days = {horizon}
      AND mr.status = {_sql_literal(status)}
      AND mr.feature_set_hash = {_sql_literal(feature_set_hash)}
    ORDER BY br.finished_at DESC NULLS LAST, br.started_at DESC, br.id DESC
    LIMIT 1
) row;
""".strip()
    )
    if not isinstance(rows, list):
        raise FalsifierCliError("run identity query returned non-list")
    if not rows:
        return None
    row = rows[0]
    if not isinstance(row, Mapping):
        raise FalsifierCliError("run identity query returned non-object row")
    return FalsifierRunIdentity(
        model_run_id=_required_int(row, "model_run_id"),
        model_run_key=_required_str(row, "model_run_key"),
        backtest_run_id=_required_int(row, "backtest_run_id"),
        backtest_run_key=_required_str(row, "backtest_run_key"),
    )


def _registry_tables_exist(client: PsqlJsonClient) -> bool:
    rows = client.fetch_json(
        """
SELECT jsonb_build_object(
    'model_runs', to_regclass('silver.model_runs') IS NOT NULL,
    'backtest_runs', to_regclass('silver.backtest_runs') IS NOT NULL
)::text;
""".strip()
    )
    if not isinstance(rows, Mapping):
        raise FalsifierCliError("registry table check returned non-object")
    model_runs = rows.get("model_runs")
    backtest_runs = rows.get("backtest_runs")
    if not isinstance(model_runs, bool) or not isinstance(backtest_runs, bool):
        raise FalsifierCliError("registry table check returned non-boolean fields")
    return model_runs and backtest_runs


def _model_run_create(
    args: argparse.Namespace,
    *,
    persisted_inputs: PersistedFalsifierInputs,
    feature_set_hash: str,
    git_sha: str,
    input_fingerprint: str,
    data_coverage: FalsifierDataCoverage,
    window: ModelRunWindow,
) -> ModelRunCreate:
    parameters = _model_run_parameters(
        args,
        persisted_inputs=persisted_inputs,
        window=window,
    )
    return ModelRunCreate(
        model_run_key=_model_run_key(
            args,
            available_at_policy_versions=persisted_inputs.available_at_policy_versions,
            feature_set_hash=feature_set_hash,
            git_sha=git_sha,
            input_fingerprint=input_fingerprint,
            target_kind=persisted_inputs.target_kind,
            window=window,
        ),
        name=FALSIFIER_MODEL_RUN_NAME,
        code_git_sha=git_sha,
        feature_set_hash=feature_set_hash,
        training_start_date=window.training_start_date,
        training_end_date=window.training_end_date,
        test_start_date=window.test_start_date,
        test_end_date=window.test_end_date,
        horizon_days=args.horizon,
        target_kind=persisted_inputs.target_kind,
        random_seed=FALSIFIER_RANDOM_SEED,
        cost_assumptions=_model_run_cost_assumptions(),
        parameters=parameters,
        available_at_policy_versions=(
            persisted_inputs.available_at_policy_versions
        ),
        input_fingerprints=_model_run_input_fingerprints(
            persisted_inputs=persisted_inputs,
            input_fingerprint=input_fingerprint,
            data_coverage=data_coverage,
        ),
    )


def _model_run_parameters(
    args: argparse.Namespace,
    *,
    persisted_inputs: PersistedFalsifierInputs,
    window: ModelRunWindow,
) -> dict[str, object]:
    return {
        "command": _target_command(args),
        "feature_definition": _feature_definition_parameters(
            persisted_inputs.feature_definition,
        ),
        "min_train_sessions": DEFAULT_MIN_TRAIN_SESSIONS,
        "step_sessions": DEFAULT_STEP_SESSIONS,
        "strategy": args.strategy,
        "test_sessions": DEFAULT_TEST_SESSIONS,
        "universe": args.universe,
        "window_source": window.source,
    }


def _feature_definition_parameters(
    feature_definition: FeatureDefinitionRecord,
) -> dict[str, object]:
    return {
        "definition_hash": feature_definition.definition_hash,
        "name": feature_definition.name,
        "version": feature_definition.version,
    }


def _model_run_input_fingerprints(
    *,
    persisted_inputs: PersistedFalsifierInputs,
    input_fingerprint: str,
    data_coverage: FalsifierDataCoverage,
) -> dict[str, object]:
    return {
        "asof_end": (
            None
            if data_coverage.asof_end is None
            else data_coverage.asof_end.isoformat()
        ),
        "asof_start": (
            None
            if data_coverage.asof_start is None
            else data_coverage.asof_start.isoformat()
        ),
        "distinct_asof_dates": data_coverage.distinct_asof_dates,
        "distinct_tickers": data_coverage.distinct_tickers,
        "horizon_end": (
            None
            if data_coverage.horizon_end is None
            else data_coverage.horizon_end.isoformat()
        ),
        "horizon_start": (
            None
            if data_coverage.horizon_start is None
            else data_coverage.horizon_start.isoformat()
        ),
        "joined_feature_label_rows_sha256": input_fingerprint,
        "row_count": data_coverage.input_rows,
        "universe_member_count": len(persisted_inputs.universe_members),
    }


def _model_run_key(
    args: argparse.Namespace,
    *,
    available_at_policy_versions: Mapping[str, int],
    feature_set_hash: str,
    git_sha: str,
    input_fingerprint: str,
    target_kind: str,
    window: ModelRunWindow,
) -> str:
    payload = {
        "available_at_policy_versions": dict(available_at_policy_versions),
        "contract": "falsifier-model-run-identity",
        "cost_assumptions": _model_run_cost_assumptions(),
        "feature_set_hash": feature_set_hash,
        "git_sha": git_sha,
        "horizon": args.horizon,
        "input_fingerprint": input_fingerprint,
        "run_config": {
            "min_train_sessions": DEFAULT_MIN_TRAIN_SESSIONS,
            "step_sessions": DEFAULT_STEP_SESSIONS,
            "strategy": args.strategy,
            "test_sessions": DEFAULT_TEST_SESSIONS,
            "universe": args.universe,
        },
        "random_seed": FALSIFIER_RANDOM_SEED,
        "target_kind": target_kind,
        "test_end_date": window.test_end_date.isoformat(),
        "test_start_date": window.test_start_date.isoformat(),
        "training_end_date": window.training_end_date.isoformat(),
        "training_start_date": window.training_start_date.isoformat(),
        "window_source": window.source,
        "version": 3,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return (
        f"falsifier-{args.strategy}-{args.universe}-{args.horizon}-"
        f"{git_sha[:12]}-{digest[:16]}"
    )


def _model_run_window(
    rows: Sequence[MomentumBacktestRow],
    *,
    calendar: TradingCalendar,
    horizon: int,
) -> ModelRunWindow:
    normalized_rows = tuple(rows)
    if not normalized_rows:
        raise FalsifierCliError(
            "cannot create model-run metadata without joined falsifier rows"
        )

    splits = _walk_forward_splits_covering_rows(
        normalized_rows,
        calendar=calendar,
        horizon=horizon,
    )
    scorable_splits = _scorable_splits(normalized_rows, splits)
    if scorable_splits:
        return _window_from_splits(scorable_splits, source="scorable_walk_forward")
    if splits:
        return _window_from_splits(splits, source="planned_walk_forward")
    return _fallback_window_from_rows(normalized_rows, calendar=calendar)


def _walk_forward_splits_covering_rows(
    rows: Sequence[MomentumBacktestRow],
    *,
    calendar: TradingCalendar,
    horizon: int,
) -> tuple[WalkForwardSplit, ...]:
    start = min(row.asof_date for row in rows)
    end = max(row.horizon_date for row in rows)
    calendar_rows = tuple(row for row in calendar.rows if start <= row.date <= end)
    return plan_walk_forward_splits(
        calendar_rows,
        WalkForwardConfig(
            min_train_sessions=DEFAULT_MIN_TRAIN_SESSIONS,
            test_sessions=DEFAULT_TEST_SESSIONS,
            step_sessions=DEFAULT_STEP_SESSIONS,
            label_horizon_sessions=horizon,
        ),
    )


def _scorable_splits(
    rows: Sequence[MomentumBacktestRow],
    splits: Sequence[WalkForwardSplit],
) -> tuple[WalkForwardSplit, ...]:
    rows_by_date: dict[date, int] = {}
    for row in rows:
        rows_by_date[row.asof_date] = rows_by_date.get(row.asof_date, 0) + 1

    scorable: list[WalkForwardSplit] = []
    for split in splits:
        train_observations = sum(
            rows_by_date.get(session, 0) for session in split.train_sessions
        )
        test_observations = sum(
            rows_by_date.get(session, 0) for session in split.test_sessions
        )
        if train_observations and test_observations:
            scorable.append(split)
    return tuple(scorable)


def _window_from_splits(
    splits: Sequence[WalkForwardSplit],
    *,
    source: str,
) -> ModelRunWindow:
    first = splits[0]
    last = splits[-1]
    return ModelRunWindow(
        training_start_date=first.train_start,
        training_end_date=first.train_end,
        test_start_date=first.test_start,
        test_end_date=last.test_end,
        source=source,
    )


def _fallback_window_from_rows(
    rows: Sequence[MomentumBacktestRow],
    *,
    calendar: TradingCalendar,
) -> ModelRunWindow:
    training_start = min(row.asof_date for row in rows)
    training_end = max(row.asof_date for row in rows)
    test_start = _next_session_after(calendar, training_end)
    test_end = max(max(row.horizon_date for row in rows), test_start)
    return ModelRunWindow(
        training_start_date=training_start,
        training_end_date=training_end,
        test_start_date=test_start,
        test_end_date=test_end,
        source="input_coverage_fallback",
    )


def _next_session_after(calendar: TradingCalendar, day: date) -> date:
    for row in sorted(calendar.rows, key=lambda item: item.date):
        if row.is_session and row.date > day:
            return row.date
    return day + timedelta(days=1)


def _model_run_metrics(result: Any) -> Mapping[str, Any]:
    metrics = result.headline_metrics
    return {
        "eligible_observations": metrics.eligible_observations,
        "failure_modes": list(result.failure_modes),
        "mean_baseline_gross_return": metrics.mean_baseline_gross_return,
        "mean_baseline_net_return": metrics.mean_baseline_net_return,
        "mean_net_difference_vs_baseline": (
            metrics.mean_net_difference_vs_baseline
        ),
        "mean_strategy_gross_return": metrics.mean_strategy_gross_return,
        "mean_strategy_net_return": metrics.mean_strategy_net_return,
        "round_trip_cost_bps": result.round_trip_cost_bps,
        "scored_test_dates": metrics.scored_test_dates,
        "selected_observations": metrics.selected_observations,
        "split_count": metrics.split_count,
        "status": result.status,
        "strategy_net_hit_rate": metrics.strategy_net_hit_rate,
        "strategy_net_return_stddev": metrics.strategy_net_return_stddev,
        "strategy_net_return_to_stddev": metrics.strategy_net_return_to_stddev,
    }


def validate_falsifier_report_traceability(
    report: FalsifierReport,
    metadata_repository: BacktestMetadataRepository,
    *,
    expected_backtest_finish: BacktestRunFinish | None = None,
) -> None:
    """Validate that a generated report resolves to matching durable metadata."""
    identity = report.reproducibility.run_identity
    if identity is None:
        raise FalsifierCliError(
            "falsifier report traceability validation requires model_run_id "
            "and backtest_run_id"
        )

    snapshot = metadata_repository.load_backtest_traceability_snapshot(
        identity.backtest_run_id,
    )
    mismatches = _traceability_mismatches(
        report,
        identity,
        snapshot,
        expected_backtest_finish=expected_backtest_finish,
    )
    if mismatches:
        raise FalsifierCliError(
            "falsifier report traceability validation failed: "
            + "; ".join(mismatches)
        )


def _traceability_mismatches(
    report: FalsifierReport,
    identity: FalsifierRunIdentity,
    snapshot: BacktestTraceabilitySnapshot,
    *,
    expected_backtest_finish: BacktestRunFinish | None = None,
) -> list[str]:
    result = report.backtest_result
    mismatches: list[str] = []
    checks: list[tuple[str, object, object]] = [
        ("model_runs.id", snapshot.model_run_id, identity.model_run_id),
        (
            "model_runs.model_run_key",
            snapshot.model_run_key,
            identity.model_run_key,
        ),
        ("model_runs.status", snapshot.model_status, result.status),
        (
            "model_runs.code_git_sha",
            snapshot.model_code_git_sha,
            report.reproducibility.git_sha,
        ),
        (
            "model_runs.feature_set_hash",
            snapshot.model_feature_set_hash,
            report.feature_metadata.feature_set_hash,
        ),
        ("model_runs.horizon_days", snapshot.model_horizon_days, report.horizon),
        (
            "model_runs.target_kind",
            snapshot.model_target_kind,
            report.reproducibility.target_kind,
        ),
        (
            "model_runs.random_seed",
            snapshot.model_random_seed,
            report.reproducibility.random_seed,
        ),
        (
            "model_runs.cost_assumptions",
            snapshot.model_cost_assumptions,
            _model_run_cost_assumptions(),
        ),
        ("model_runs.metrics", snapshot.model_metrics, _model_run_metrics(result)),
        (
            "model_runs.available_at_policy_versions",
            snapshot.model_available_at_policy_versions,
            report.reproducibility.available_at_policy_versions,
        ),
        (
            "model_runs.input_fingerprints.joined_feature_label_rows_sha256",
            snapshot.model_input_fingerprints.get(
                "joined_feature_label_rows_sha256",
            ),
            report.reproducibility.input_fingerprint,
        ),
        (
            "report.execution_assumptions",
            report.reproducibility.execution_assumptions,
            _execution_assumptions_from_snapshot(snapshot),
        ),
        ("backtest_runs.id", snapshot.backtest_run_id, identity.backtest_run_id),
        (
            "backtest_runs.backtest_run_key",
            snapshot.backtest_run_key,
            identity.backtest_run_key,
        ),
        ("backtest_runs.status", snapshot.backtest_status, result.status),
        (
            "backtest_runs.model_run_id",
            snapshot.backtest_model_run_id,
            identity.model_run_id,
        ),
        (
            "backtest_runs.universe_name",
            snapshot.backtest_universe_name,
            report.universe_name,
        ),
        ("backtest_runs.horizon_days", snapshot.backtest_horizon_days, report.horizon),
        (
            "backtest_runs.target_kind",
            snapshot.backtest_target_kind,
            snapshot.model_target_kind,
        ),
        (
            "backtest_runs.cost_assumptions",
            snapshot.backtest_cost_assumptions,
            _cost_assumptions(result),
        ),
        (
            "backtest_runs.metrics",
            snapshot.backtest_metrics,
            _metrics_payload(result=result, failure_message=None),
        ),
        (
            "backtest_runs.metrics_by_regime",
            snapshot.backtest_metrics_by_regime,
            _metrics_by_regime(result),
        ),
        (
            "backtest_runs.baseline_metrics",
            snapshot.backtest_baseline_metrics,
            _baseline_metrics(result),
        ),
        (
            "backtest_runs.multiple_comparisons_correction",
            snapshot.backtest_multiple_comparisons_correction,
            MULTIPLE_COMPARISONS_CORRECTION,
        ),
    ]

    model_window = report.reproducibility.model_window
    if model_window is None:
        mismatches.append("report reproducibility metadata missing model window")
    else:
        checks.extend(
            [
                (
                    "model_runs.training_start_date",
                    snapshot.model_training_start_date,
                    model_window.training_start_date,
                ),
                (
                    "model_runs.training_end_date",
                    snapshot.model_training_end_date,
                    model_window.training_end_date,
                ),
                (
                    "model_runs.test_start_date",
                    snapshot.model_test_start_date,
                    model_window.test_start_date,
                ),
                (
                    "model_runs.test_end_date",
                    snapshot.model_test_end_date,
                    model_window.test_end_date,
                ),
                (
                    "model_runs.parameters.window_source",
                    snapshot.model_parameters.get("window_source"),
                    model_window.source,
                ),
            ]
        )

    for field, actual, expected in checks:
        _expect_trace_value(mismatches, field, actual=actual, expected=expected)
    if expected_backtest_finish is not None:
        label_checks = (
            (
                "backtest_runs.label_scramble_metrics",
                snapshot.backtest_label_scramble_metrics,
                expected_backtest_finish.label_scramble_metrics,
            ),
            (
                "backtest_runs.label_scramble_pass",
                snapshot.backtest_label_scramble_pass,
                expected_backtest_finish.label_scramble_pass,
            ),
        )
        for field, actual, expected in label_checks:
            _expect_trace_value(mismatches, field, actual=actual, expected=expected)
    return mismatches


def _execution_assumptions_from_snapshot(
    snapshot: BacktestTraceabilitySnapshot,
) -> dict[str, object]:
    return {
        "label_scramble_alpha": snapshot.backtest_parameters.get(
            "label_scramble_alpha"
        ),
        "label_scramble_seed": snapshot.backtest_parameters.get(
            "label_scramble_seed"
        ),
        "label_scramble_trial_count": snapshot.backtest_parameters.get(
            "label_scramble_trial_count"
        ),
        "min_train_sessions": snapshot.model_parameters.get("min_train_sessions"),
        "multiple_comparisons_correction": (
            snapshot.backtest_multiple_comparisons_correction
        ),
        "round_trip_cost_bps": snapshot.backtest_cost_assumptions.get(
            "round_trip_cost_bps"
        ),
        "step_sessions": snapshot.model_parameters.get("step_sessions"),
        "test_sessions": snapshot.model_parameters.get("test_sessions"),
    }


def _expect_trace_value(
    mismatches: list[str],
    field: str,
    *,
    actual: object,
    expected: object,
) -> None:
    normalized_actual = _trace_normalize(actual)
    normalized_expected = _trace_normalize(expected)
    if normalized_actual != normalized_expected:
        mismatches.append(
            f"{field} expected {_trace_token(normalized_expected)} "
            f"got {_trace_token(normalized_actual)}"
        )


def _trace_normalize(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            str(key): _trace_normalize(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, tuple):
        return [_trace_normalize(item) for item in value]
    if isinstance(value, list):
        return [_trace_normalize(item) for item in value]
    return value


def _trace_token(value: object) -> str:
    if isinstance(value, (Mapping, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return repr(value)


def _validate_args(args: argparse.Namespace) -> None:
    if args.horizon not in CANONICAL_HORIZONS:
        allowed = ", ".join(str(horizon) for horizon in CANONICAL_HORIZONS)
        raise FalsifierCliError(f"horizon must be one of {allowed}; got {args.horizon}")
    if not isinstance(args.universe, str) or not args.universe.strip():
        raise FalsifierCliError("universe must be a non-empty string")
    _validate_report_path(args.output_path)


def _validate_report_path(path: Path) -> None:
    report_path = _resolve_repo_path(path)
    try:
        report_path.relative_to(ROOT)
    except ValueError as exc:
        raise FalsifierCliError("output path must be inside this repository") from exc
    if report_path.suffix != ".md":
        raise FalsifierCliError("output path must be a markdown file")


def _resolve_repo_path(path: Path) -> Path:
    if path.is_absolute():
        return path.resolve()
    return (ROOT / path).resolve()


def _display_path(path: Path) -> str:
    resolved = _resolve_repo_path(path)
    try:
        return str(resolved.relative_to(ROOT))
    except ValueError:
        return str(resolved)


def _target_command(args: argparse.Namespace) -> str:
    return TARGET_COMMAND_TEMPLATE.format(
        strategy=args.strategy,
        horizon=args.horizon,
        universe=args.universe,
    )


def _git_sha() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise FalsifierCliError("could not resolve git SHA for reproducibility metadata")
    return result.stdout.strip()


def _connect_metadata_database(database_url: str) -> Any:
    try:
        import psycopg
    except ImportError as exc:
        raise FalsifierCliError(
            "psycopg is required to write model-run metadata; run `uv sync`"
        ) from exc
    try:
        return psycopg.connect(database_url)
    except Exception as exc:
        raise FalsifierCliError(
            "could not connect to Postgres for metadata writes"
        ) from exc


def _commit(connection: Any) -> None:
    commit = getattr(connection, "commit", None)
    if commit is not None:
        commit()


def _close(connection: Any) -> None:
    close = getattr(connection, "close", None)
    if close is not None:
        close()


def _feature_set_hash(feature: FeatureDefinitionRecord) -> str:
    payload = (
        f"{feature.name}:"
        f"{feature.version}:"
        f"{feature.definition_hash}"
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FalsifierCliError(f"{name} must be a positive integer")
    return value


def _sql_required_str(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FalsifierCliError(f"{name} must be a non-empty string")
    return value.strip()


def _required_str(row: Mapping[str, Any], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise FalsifierCliError(f"persisted row field `{key}` must be a string")
    return value.strip()


def _required_int(row: Mapping[str, Any], key: str) -> int:
    value = row.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise FalsifierCliError(f"persisted row field `{key}` must be an integer")
    return value


def _optional_date(value: object) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise FalsifierCliError("optional persisted date field must be a string or null")
    return date.fromisoformat(value)


if __name__ == "__main__":
    raise SystemExit(main())
