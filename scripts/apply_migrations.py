#!/usr/bin/env python
"""Check and apply Silver Postgres migrations."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MIGRATIONS_DIR = ROOT / "db" / "migrations"
MIGRATION_RE = re.compile(r"^(?P<version>\d{3})_(?P<name>[a-z0-9_]+)\.sql$")
TABLE_RE = re.compile(r"CREATE\s+TABLE\s+silver\.([a-z_]+)\s*\(", re.I)
FOUNDATION_TABLES = (
    "securities",
    "security_identifiers",
    "trading_calendar",
    "universe_membership",
    "raw_objects",
    "available_at_policies",
)
FOUNDATION_COLUMNS = {
    "securities": (
        "ticker",
        "name",
        "cik",
        "exchange",
        "asset_class",
        "country",
        "currency",
        "fiscal_year_end_md",
        "listed_at",
        "delisted_at",
        "created_at",
        "updated_at",
    ),
    "security_identifiers": (
        "security_id",
        "identifier_type",
        "identifier",
        "valid_from",
        "valid_to",
    ),
    "trading_calendar": (
        "date",
        "is_session",
        "session_close",
        "is_early_close",
    ),
    "universe_membership": (
        "security_id",
        "universe_name",
        "valid_from",
        "valid_to",
        "reason",
    ),
    "raw_objects": (
        "vendor",
        "endpoint",
        "params_hash",
        "params",
        "request_url",
        "http_status",
        "content_type",
        "body_jsonb",
        "body_raw",
        "raw_hash",
        "fetched_at",
    ),
    "available_at_policies": (
        "name",
        "version",
        "rule",
        "valid_from",
        "valid_to",
        "notes",
    ),
}
PHASE1_ANALYTICS_MIGRATION = "003_phase1_analytics.sql"
PHASE1_ANALYTICS_TABLES = (
    "analytics_runs",
    "prices_daily",
    "feature_definitions",
    "feature_values",
    "forward_return_labels",
)
PHASE1_ANALYTICS_COLUMNS = {
    "analytics_runs": (
        "run_kind",
        "code_git_sha",
        "feature_set_hash",
        "available_at_policy_versions",
        "parameters",
        "input_fingerprints",
        "random_seed",
        "started_at",
        "finished_at",
        "status",
    ),
    "prices_daily": (
        "security_id",
        "date",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "currency",
        "source_system",
        "normalization_version",
        "available_at",
        "available_at_policy_id",
        "raw_object_id",
        "normalized_by_run_id",
        "created_at",
    ),
    "feature_definitions": (
        "name",
        "version",
        "kind",
        "computation_spec",
        "definition_hash",
        "notes",
        "created_at",
    ),
    "feature_values": (
        "security_id",
        "asof_date",
        "feature_definition_id",
        "value",
        "available_at",
        "available_at_policy_id",
        "computed_by_run_id",
        "computed_at",
        "source_metadata",
    ),
    "forward_return_labels": (
        "security_id",
        "label_date",
        "horizon_days",
        "horizon_date",
        "horizon_close_at",
        "label_version",
        "start_adj_close",
        "end_adj_close",
        "realized_raw_return",
        "benchmark_security_id",
        "realized_excess_return",
        "available_at",
        "available_at_policy_id",
        "computed_by_run_id",
        "computed_at",
        "metadata",
    ),
}
PHASE1_ANALYTICS_REQUIRED_SNIPPETS = (
    "primary key (security_id, date)",
    "unique (name, version)",
    "unique (security_id, asof_date, feature_definition_id)",
    "unique (security_id, label_date, horizon_days, label_version)",
    "check (horizon_days in (5, 21, 63, 126, 252))",
    "references silver.raw_objects(id)",
    "references silver.available_at_policies(id)",
    "references silver.feature_definitions(id) on delete restrict",
    "feature_definitions_immutable_when_referenced",
)


class MigrationError(RuntimeError):
    """Raised when migration discovery, validation, or apply fails."""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.path.read_bytes()).hexdigest()

    @property
    def sql(self) -> str:
        return self.path.read_text(encoding="utf-8")


def discover_migrations(migrations_dir: Path = DEFAULT_MIGRATIONS_DIR) -> list[Migration]:
    if not migrations_dir.exists():
        raise MigrationError(f"migration directory does not exist: {migrations_dir}")

    migrations: list[Migration] = []
    invalid_names: list[str] = []
    for path in sorted(migrations_dir.glob("*.sql")):
        match = MIGRATION_RE.fullmatch(path.name)
        if match is None:
            invalid_names.append(path.name)
            continue
        migrations.append(
            Migration(
                version=int(match.group("version")),
                name=match.group("name"),
                path=path,
            )
        )

    if invalid_names:
        names = ", ".join(invalid_names)
        raise MigrationError(f"invalid migration filename(s): {names}")
    if not migrations:
        raise MigrationError(f"no migrations found in {migrations_dir}")

    versions = [migration.version for migration in migrations]
    expected_versions = list(range(1, len(migrations) + 1))
    if versions != expected_versions:
        raise MigrationError(
            "migration versions must be contiguous starting at 001; "
            f"found {versions}, expected {expected_versions}"
        )

    return migrations


def validate_static_schema(migrations: Sequence[Migration]) -> None:
    foundation = migrations[0]
    if foundation.path.name != "001_foundation.sql":
        raise MigrationError("first migration must be db/migrations/001_foundation.sql")

    sql = foundation.sql
    normalized_sql = _normalize_sql(sql)
    if "create schema if not exists silver" not in normalized_sql:
        raise MigrationError("001_foundation.sql must create the silver schema")

    tables = tuple(match.lower() for match in TABLE_RE.findall(sql))
    if tables != FOUNDATION_TABLES:
        raise MigrationError(
            "001_foundation.sql must create exactly the foundation tables "
            f"{FOUNDATION_TABLES}; found {tables}"
        )

    for table, columns in FOUNDATION_COLUMNS.items():
        body = _table_body(sql, table)
        for column in columns:
            if not re.search(rf"\b{re.escape(column)}\b", body, re.I):
                raise MigrationError(f"silver.{table} is missing column {column}")

    if "unique (name, version)" not in normalized_sql:
        raise MigrationError("available_at_policies must version policies by name")

    for temporal_table in ("security_identifiers", "universe_membership"):
        body = _table_body(sql, temporal_table)
        if "valid_from" not in body.lower() or "valid_to" not in body.lower():
            raise MigrationError(f"silver.{temporal_table} must carry valid ranges")

    if len(migrations) >= 3:
        validate_phase1_analytics_schema(migrations[2])


def validate_phase1_analytics_schema(migration: Migration) -> None:
    if migration.path.name != PHASE1_ANALYTICS_MIGRATION:
        raise MigrationError(
            f"third migration must be db/migrations/{PHASE1_ANALYTICS_MIGRATION}"
        )

    sql = migration.sql
    tables = tuple(match.lower() for match in TABLE_RE.findall(sql))
    if tables != PHASE1_ANALYTICS_TABLES:
        raise MigrationError(
            f"{PHASE1_ANALYTICS_MIGRATION} must create exactly the Phase 1 "
            f"analytics tables {PHASE1_ANALYTICS_TABLES}; found {tables}"
        )

    for table, columns in PHASE1_ANALYTICS_COLUMNS.items():
        body = _table_body(sql, table)
        for column in columns:
            if not re.search(rf"\b{re.escape(column)}\b", body, re.I):
                raise MigrationError(f"silver.{table} is missing column {column}")

    normalized_sql = _normalize_sql(sql)
    for snippet in PHASE1_ANALYTICS_REQUIRED_SNIPPETS:
        if snippet not in normalized_sql:
            raise MigrationError(
                f"{PHASE1_ANALYTICS_MIGRATION} is missing required SQL: {snippet}"
            )


def check_migrations(migrations_dir: Path = DEFAULT_MIGRATIONS_DIR) -> list[Migration]:
    migrations = discover_migrations(migrations_dir)
    validate_static_schema(migrations)
    return migrations


def apply_migrations(
    migrations: Sequence[Migration],
    database_url: str,
    *,
    psql_path: str | None = None,
) -> list[Migration]:
    psql = psql_path or shutil.which("psql")
    if psql is None:
        raise MigrationError("psql is required to apply migrations")

    _run_psql(psql, database_url, TRACKING_TABLE_SQL)
    applied = _load_applied_migrations(psql, database_url)
    applied_versions = set(applied)

    for version, checksum in applied.items():
        matching = next(
            (migration for migration in migrations if migration.version == version),
            None,
        )
        if matching is not None and matching.checksum != checksum:
            raise MigrationError(
                f"applied migration {version:03d} checksum does not match local file"
            )

    pending = [
        migration for migration in migrations if migration.version not in applied_versions
    ]
    for migration in pending:
        _apply_one_migration(psql, database_url, migration)
    return pending


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate migration ordering and static SQL without connecting to Postgres",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL; defaults to DATABASE_URL",
    )
    parser.add_argument(
        "--migrations-dir",
        type=Path,
        default=DEFAULT_MIGRATIONS_DIR,
        help="directory containing numbered SQL migrations",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        migrations = check_migrations(args.migrations_dir)
        if args.check:
            print(f"OK: {len(migrations)} migration(s) checked")
            return 0
        if not args.database_url:
            raise MigrationError("DATABASE_URL is required unless --check is used")
        pending = apply_migrations(migrations, args.database_url)
    except MigrationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if pending:
        versions = ", ".join(f"{migration.version:03d}" for migration in pending)
        print(f"OK: applied migration(s): {versions}")
    else:
        print("OK: no pending migrations")
    return 0


TRACKING_TABLE_SQL = """
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.schema_migrations (
    version integer PRIMARY KEY,
    name text NOT NULL,
    checksum text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);
"""


def _apply_one_migration(psql: str, database_url: str, migration: Migration) -> None:
    sql = f"""
BEGIN;

{migration.sql}

INSERT INTO silver.schema_migrations (version, name, checksum)
VALUES (
    {migration.version},
    {_sql_literal(migration.path.name)},
    {_sql_literal(migration.checksum)}
);

COMMIT;
"""
    _run_psql(psql, database_url, sql)


def _load_applied_migrations(psql: str, database_url: str) -> dict[int, str]:
    output = _run_psql(
        psql,
        database_url,
        "SELECT version, checksum FROM silver.schema_migrations ORDER BY version;",
        capture=True,
    )
    applied: dict[int, str] = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        version, checksum = line.split("\t", maxsplit=1)
        applied[int(version)] = checksum
    return applied


def _run_psql(
    psql: str,
    database_url: str,
    sql: str,
    *,
    capture: bool = False,
) -> str:
    command = [psql, "-X", "-v", "ON_ERROR_STOP=1", "-q", "-d", database_url]
    if capture:
        command.extend(["-A", "-t", "-F", "\t", "-c", sql])
        input_sql = None
    else:
        input_sql = sql

    result = subprocess.run(
        command,
        input=input_sql,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.replace(database_url, "[DATABASE_URL]").strip()
        detail = f": {stderr}" if stderr else ""
        raise MigrationError(f"psql failed with exit code {result.returncode}{detail}")
    return result.stdout


def _normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.lower())


def _table_body(sql: str, table: str) -> str:
    match = re.search(
        rf"CREATE\s+TABLE\s+silver\.{re.escape(table)}\s*\((.*?)\)\s*;",
        sql,
        flags=re.I | re.S,
    )
    if match is None:
        raise MigrationError(f"silver.{table} table definition is missing")
    return match.group(1)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


if __name__ == "__main__":
    raise SystemExit(main())
