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
