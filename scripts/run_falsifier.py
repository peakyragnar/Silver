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
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from silver.backtest.momentum_falsifier import (  # noqa: E402
    DEFAULT_MIN_TRAIN_SESSIONS,
    DEFAULT_ROUND_TRIP_COST_BPS,
    DEFAULT_STEP_SESSIONS,
    DEFAULT_TEST_SESSIONS,
    MomentumBacktestRow,
    MomentumFalsifierInputError,
    run_momentum_falsifier,
)
from silver.features.momentum_12_1 import MOMENTUM_12_1_DEFINITION  # noqa: E402
from silver.reference.seed_data import (  # noqa: E402
    DEFAULT_CONFIG_PATH as DEFAULT_REFERENCE_CONFIG_PATH,
)
from silver.reference.seed_data import FALSIFIER_UNIVERSE_NAME, load_seed_file  # noqa: E402
from silver.reports.falsifier import (  # noqa: E402
    FalsifierFeatureMetadata,
    FalsifierInputCounts,
    FalsifierReport,
    FalsifierReproducibilityMetadata,
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
    persisted_inputs = load_persisted_inputs(
        client,
        strategy=args.strategy,
        horizon=args.horizon,
        universe=args.universe,
    )
    result = run_momentum_falsifier(
        persisted_inputs.rows,
        calendar=calendar,
        horizon_sessions=args.horizon,
        min_train_sessions=DEFAULT_MIN_TRAIN_SESSIONS,
        test_sessions=DEFAULT_TEST_SESSIONS,
        step_sessions=DEFAULT_STEP_SESSIONS,
        round_trip_cost_bps=DEFAULT_ROUND_TRIP_COST_BPS,
    )
    feature = persisted_inputs.feature_definition
    report = FalsifierReport(
        strategy=args.strategy,
        horizon=args.horizon,
        universe_name=args.universe,
        universe_members=persisted_inputs.universe_members,
        data_coverage=coverage_from_rows(persisted_inputs.rows),
        feature_metadata=FalsifierFeatureMetadata(
            name=feature.name,
            version=feature.version,
            definition_hash=feature.definition_hash,
            feature_set_hash=_feature_set_hash(feature),
        ),
        backtest_result=result,
        reproducibility=FalsifierReproducibilityMetadata(
            command=_target_command(args),
            git_sha=_git_sha(),
            input_fingerprint=fingerprint_momentum_inputs(persisted_inputs.rows),
            available_at_policy_versions=persisted_inputs.available_at_policy_versions,
        ),
    )
    write_report(args.output_path, render_week_1_momentum_report(report))
    print(f"OK: wrote {_display_path(args.output_path)} with status {result.status}")


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
