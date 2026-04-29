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
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from silver.analytics import (  # noqa: E402
    BacktestMetadataRepository,
    ModelRunCreate,
    ModelRunFinish,
    ModelRunRecord,
)
from silver.backtest.momentum_falsifier import (  # noqa: E402
    DEFAULT_MIN_TRAIN_SESSIONS,
    DEFAULT_ROUND_TRIP_COST_BPS,
    DEFAULT_STEP_SESSIONS,
    DEFAULT_TEST_SESSIONS,
    MomentumBacktestRow,
    MomentumFalsifierInputError,
    run_momentum_falsifier,
)
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
    FalsifierFeatureMetadata,
    FalsifierInputCounts,
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
FALSIFIER_RANDOM_SEED = 0
TARGET_COMMAND_TEMPLATE = (
    "python scripts/run_falsifier.py --strategy {strategy} --horizon {horizon} "
    "--universe {universe}"
)


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
        f"{report_run.status}; model_run_id={report_run.model_run.id}"
    )


def run_report_with_metadata(
    args: argparse.Namespace,
    *,
    client: PsqlJsonClient,
    metadata_repository: BacktestMetadataRepository,
    calendar: TradingCalendar,
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
    model_run = metadata_repository.create_model_run(
        _model_run_create(
            args,
            persisted_inputs=persisted_inputs,
            feature_set_hash=feature_set_hash,
            git_sha=git_sha,
            input_fingerprint=input_fingerprint,
            data_coverage=data_coverage,
            calendar=calendar,
        )
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
                available_at_policy_versions=(
                    persisted_inputs.available_at_policy_versions
                ),
                run_identity=_load_run_identity(
                    client,
                    horizon=args.horizon,
                    universe=args.universe,
                    feature_set_hash=feature_set_hash,
                    status=result.status,
                ),
                random_seed=FALSIFIER_RANDOM_SEED,
            ),
        )
        write_report(args.output_path, render_week_1_momentum_report(report))
    except Exception as exc:
        metadata_repository.finish_model_run(
            model_run.id,
            ModelRunFinish(
                status="failed",
                metrics={
                    "error_message": str(exc),
                    "error_type": type(exc).__name__,
                },
            ),
        )
        raise

    finished = metadata_repository.finish_model_run(
        model_run.id,
        ModelRunFinish(status=result.status, metrics=_model_run_metrics(result)),
    )
    return FalsifierReportRun(model_run=finished, status=result.status)


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
        available_at_policy_versions=_load_policy_versions(client),
        target_kind=_load_target_kind(
            client,
            feature_definition_id=feature_definition.id,
            horizon=horizon,
            universe=universe,
        ),
    )


def write_report(path: Path, content: str) -> None:
    report_path = _resolve_repo_path(path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(content, encoding="utf-8")


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


def _load_policy_versions(client: PsqlJsonClient) -> Mapping[str, int]:
    rows = client.fetch_json(
        """
SELECT COALESCE(jsonb_object_agg(name, version), '{}'::jsonb)::text
FROM silver.available_at_policies
WHERE valid_to IS NULL;
""".strip()
    )
    if not isinstance(rows, Mapping):
        raise FalsifierCliError("available_at policy versions query returned non-object")
    return {str(key): int(value) for key, value in rows.items()}


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
    calendar: TradingCalendar,
) -> ModelRunCreate:
    window = _model_run_window(
        persisted_inputs.rows,
        calendar=calendar,
        horizon=args.horizon,
    )
    return ModelRunCreate(
        model_run_key=_model_run_key(args, git_sha, input_fingerprint),
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
        cost_assumptions={
            "application": (
                "subtracted from strategy and equal-weight baseline returns "
                "for each scored test date"
            ),
            "round_trip_cost_bps": DEFAULT_ROUND_TRIP_COST_BPS,
        },
        parameters={
            "command": _target_command(args),
            "feature_definition": {
                "definition_hash": persisted_inputs.feature_definition.definition_hash,
                "id": persisted_inputs.feature_definition.id,
                "name": persisted_inputs.feature_definition.name,
                "version": persisted_inputs.feature_definition.version,
            },
            "min_train_sessions": DEFAULT_MIN_TRAIN_SESSIONS,
            "output_path": _display_path(args.output_path),
            "step_sessions": DEFAULT_STEP_SESSIONS,
            "strategy": args.strategy,
            "test_sessions": DEFAULT_TEST_SESSIONS,
            "universe": args.universe,
            "window_source": window.source,
        },
        available_at_policy_versions=(
            persisted_inputs.available_at_policy_versions
        ),
        input_fingerprints={
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
        },
    )


def _model_run_key(
    args: argparse.Namespace,
    git_sha: str,
    input_fingerprint: str,
) -> str:
    invocation_id = uuid.uuid4().hex
    payload = {
        "git_sha": git_sha,
        "horizon": args.horizon,
        "input_fingerprint": input_fingerprint,
        "invocation_id": invocation_id,
        "strategy": args.strategy,
        "universe": args.universe,
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
