from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_SCRIPT = ROOT / "scripts" / "apply_migrations.py"


def load_migration_module():
    spec = importlib.util.spec_from_file_location(
        "apply_migrations",
        MIGRATION_SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


apply_migrations = load_migration_module()


def test_migration_discovery_is_numbered_from_foundation() -> None:
    migrations = apply_migrations.discover_migrations(ROOT / "db" / "migrations")

    assert migrations[0].path.name == "001_foundation.sql"
    assert [migration.version for migration in migrations] == list(
        range(1, len(migrations) + 1)
    )


def test_migration_discovery_rejects_gaps(tmp_path: Path) -> None:
    (tmp_path / "001_first.sql").write_text("SELECT 1;", encoding="utf-8")
    (tmp_path / "003_third.sql").write_text("SELECT 1;", encoding="utf-8")

    with pytest.raises(apply_migrations.MigrationError, match="contiguous"):
        apply_migrations.discover_migrations(tmp_path)


def test_foundation_migration_static_schema_expectations() -> None:
    migrations = apply_migrations.check_migrations(ROOT / "db" / "migrations")
    sql = migrations[0].sql.lower()

    assert "create schema if not exists silver" in " ".join(sql.split())
    assert "silver.available_at_policies" in sql
    assert "unique (name, version)" in " ".join(sql.split())

    for table in apply_migrations.FOUNDATION_TABLES:
        assert f"create table silver.{table}" in sql


def test_foundation_migration_does_not_create_later_phase_tables() -> None:
    migrations = apply_migrations.check_migrations(ROOT / "db" / "migrations")
    created_tables = apply_migrations.TABLE_RE.findall(migrations[0].sql)

    assert tuple(created_tables) == apply_migrations.FOUNDATION_TABLES


def test_raw_objects_metadata_migration_is_additive() -> None:
    migrations = apply_migrations.check_migrations(ROOT / "db" / "migrations")
    metadata_migration = migrations[1].sql.lower()

    assert migrations[1].path.name == "002_raw_objects_metadata.sql"
    assert "alter table silver.raw_objects" in metadata_migration
    assert "add column metadata jsonb not null default '{}'::jsonb" in " ".join(
        metadata_migration.split()
    )


def test_phase1_analytics_migration_static_schema_expectations() -> None:
    migrations = apply_migrations.check_migrations(ROOT / "db" / "migrations")
    phase1 = migrations[2]
    sql = " ".join(phase1.sql.lower().split())

    assert phase1.path.name == "003_phase1_analytics.sql"
    for table in apply_migrations.PHASE1_ANALYTICS_TABLES:
        assert f"create table silver.{table}" in sql

    assert "primary key (security_id, date)" in sql
    assert "unique (name, version)" in sql
    assert "unique (security_id, asof_date, feature_definition_id)" in sql
    assert "unique (security_id, label_date, horizon_days, label_version)" in sql
    assert "check (horizon_days in (5, 21, 63, 126, 252))" in sql


def test_phase1_analytics_migration_enforces_pit_and_reproducibility() -> None:
    migrations = apply_migrations.check_migrations(ROOT / "db" / "migrations")
    sql = " ".join(migrations[2].sql.lower().split())

    for table in (
        "prices_daily",
        "feature_values",
        "forward_return_labels",
    ):
        body = apply_migrations._table_body(migrations[2].sql, table).lower()
        assert "available_at timestamptz not null" in " ".join(body.split())
        assert "available_at_policy_id bigint not null" in " ".join(body.split())

    assert "references silver.raw_objects(id)" in sql
    assert "references silver.analytics_runs(id)" in sql
    assert "feature_definitions_immutable_when_referenced" in sql
    assert "code_git_sha text not null" in sql
    assert "available_at_policy_versions jsonb not null" in sql


def test_backtest_metadata_migration_static_schema_expectations() -> None:
    migrations = apply_migrations.check_migrations(ROOT / "db" / "migrations")
    phase2 = migrations[3]
    sql = " ".join(phase2.sql.lower().split())

    assert phase2.path.name == "004_backtest_metadata.sql"
    assert len(migrations) >= 4
    for table in apply_migrations.PHASE2_BACKTEST_METADATA_TABLES:
        assert f"create table silver.{table}" in sql

    assert "references silver.model_runs(id) on delete restrict" in sql
    assert "unique (model_run_key)" in sql
    assert "unique (backtest_run_key)" in sql
    assert "horizon_days integer not null" in sql
    assert "target_kind text not null" in sql
    assert "random_seed integer not null" in sql
    assert "cost_assumptions jsonb not null default '{}'::jsonb" in sql
    assert "metrics_by_regime jsonb not null default '{}'::jsonb" in sql
    assert "label_scramble_pass boolean" in sql


def test_backtest_metadata_migration_enforces_reproducibility_constraints() -> None:
    migrations = apply_migrations.check_migrations(ROOT / "db" / "migrations")
    sql = " ".join(migrations[3].sql.lower().split())

    model_body = apply_migrations._table_body(migrations[3].sql, "model_runs")
    backtest_body = apply_migrations._table_body(migrations[3].sql, "backtest_runs")
    normalized_model = " ".join(model_body.lower().split())
    normalized_backtest = " ".join(backtest_body.lower().split())

    for body, run_key in (
        (normalized_model, "model_run_key"),
        (normalized_backtest, "backtest_run_key"),
    ):
        assert f"check (btrim({run_key}) <> '')" in body
        assert "check (jsonb_typeof(cost_assumptions) = 'object')" in body
        assert "check (jsonb_typeof(parameters) = 'object')" in body
        assert "check (jsonb_typeof(metrics) = 'object')" in body
        assert "check (finished_at is null or finished_at >= started_at)" in body
        assert "check ((status = 'running') = (finished_at is null))" in body

    assert "check (code_git_sha ~ '^[0-9a-f]{7,64}$')" in normalized_model
    assert "check (feature_set_hash ~ '^[0-9a-f]{64}$')" in normalized_model
    assert "check (training_end_date >= training_start_date)" in normalized_model
    assert "check (test_end_date >= test_start_date)" in normalized_model
    assert "check (test_start_date > training_end_date)" in normalized_model
    assert "check (random_seed >= 0)" in normalized_model
    assert "check (jsonb_typeof(available_at_policy_versions) = 'object')" in (
        normalized_model
    )
    assert "check (jsonb_typeof(input_fingerprints) = 'object')" in normalized_model

    assert "check (btrim(universe_name) <> '')" in normalized_backtest
    assert "check (jsonb_typeof(metrics_by_regime) = 'object')" in normalized_backtest
    assert "check (jsonb_typeof(baseline_metrics) = 'object')" in normalized_backtest
    assert "check (jsonb_typeof(label_scramble_metrics) = 'object')" in (
        normalized_backtest
    )
    assert (
        "check ( status not in ('succeeded', 'insufficient_data') "
        "or label_scramble_pass is not null )"
    ) in (
        normalized_backtest
    )
    assert "check (horizon_days in (5, 21, 63, 126, 252))" in sql
    assert "check (status in ('running', 'succeeded', 'failed', 'insufficient_data'))" in (
        sql
    )


def test_backtest_metadata_replay_constraints_migration_static_expectations() -> None:
    migrations = apply_migrations.check_migrations(ROOT / "db" / "migrations")
    replay = migrations[4]
    sql = apply_migrations._normalize_sql(replay.sql)

    assert replay.path.name == "005_backtest_metadata_replay_constraints.sql"
    assert len(migrations) >= 5
    for snippet in apply_migrations.PHASE2_BACKTEST_METADATA_REPLAY_REQUIRED_SNIPPETS:
        assert snippet in sql

    assert "model_runs_replay_inputs_present" in sql
    assert "model_runs_policy_versions_nonempty" in sql
    assert "backtest_runs_succeeded_claim_payloads_nonempty" in sql
    assert sql.count("not valid") == 5


def test_analytics_run_kind_expansion_migration_allows_falsifier_invocations() -> None:
    migrations = apply_migrations.check_migrations(ROOT / "db" / "migrations")
    migration = migrations[5]
    sql = apply_migrations._normalize_sql(migration.sql)

    assert migration.path.name == "006_expand_analytics_run_kinds.sql"
    assert "drop constraint analytics_runs_run_kind_check" in sql
    assert "add constraint analytics_runs_run_kind_check" in sql
    assert "'falsifier_report_invocation'" in sql
