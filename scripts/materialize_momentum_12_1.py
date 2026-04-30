#!/usr/bin/env python
"""Materialize Silver's deterministic 12-1 momentum feature."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from silver.features import (  # noqa: E402
    DAILY_PRICE_POLICY_NAME,
    DAILY_PRICE_POLICY_VERSION,
    MOMENTUM_12_1_DEFINITION,
    FeatureStoreError,
    FeatureStoreRepository,
    feature_definition_hash,
    materialize_momentum_12_1,
)
from silver.reference.seed_data import FALSIFIER_UNIVERSE_NAME  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate deterministic feature metadata without connecting to Postgres",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL; defaults to DATABASE_URL",
    )
    parser.add_argument(
        "--universe",
        default=FALSIFIER_UNIVERSE_NAME,
        help=f"universe name to materialize; defaults to {FALSIFIER_UNIVERSE_NAME}",
    )
    parser.add_argument(
        "--start-date",
        type=_date_arg,
        help="first as-of trading date to materialize, inclusive",
    )
    parser.add_argument(
        "--end-date",
        type=_date_arg,
        help="last as-of trading date to materialize, inclusive",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="compute eligible values and summary without writing feature_values",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        _validate_date_bounds(start_date=args.start_date, end_date=args.end_date)
        definition_hash = feature_definition_hash(MOMENTUM_12_1_DEFINITION)
        if args.check:
            print(
                "OK: "
                f"{MOMENTUM_12_1_DEFINITION.name} "
                f"v{MOMENTUM_12_1_DEFINITION.version} "
                f"definition hash {definition_hash} checked"
            )
            return 0

        if not args.database_url:
            raise FeatureStoreError("DATABASE_URL is required unless --check is used")

        psycopg = _load_psycopg()
        with psycopg.connect(args.database_url) as connection:
            repository = FeatureStoreRepository(connection)
            available_at_cutoff = datetime.now(timezone.utc)
            if args.dry_run:
                run_id = 1
            else:
                run_id = repository.create_feature_generation_run(
                    code_git_sha=_code_git_sha(),
                    feature_set_hash=definition_hash,
                    available_at_policy_versions={
                        DAILY_PRICE_POLICY_NAME: DAILY_PRICE_POLICY_VERSION
                    },
                    parameters={
                        "feature": MOMENTUM_12_1_DEFINITION.name,
                        "feature_version": MOMENTUM_12_1_DEFINITION.version,
                        "universe": args.universe,
                        "start_date": (
                            args.start_date.isoformat()
                            if args.start_date is not None
                            else None
                        ),
                        "end_date": (
                            args.end_date.isoformat()
                            if args.end_date is not None
                            else None
                        ),
                        "available_at_cutoff": available_at_cutoff.isoformat(),
                    },
                )
                connection.commit()

            try:
                summary = materialize_momentum_12_1(
                    repository,
                    universe_name=args.universe,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    computed_by_run_id=run_id,
                    dry_run=args.dry_run,
                    available_at_cutoff=available_at_cutoff,
                )
                if not args.dry_run:
                    repository.finish_analytics_run(run_id=run_id, status="succeeded")
            except Exception:
                if not args.dry_run:
                    connection.rollback()
                    repository.finish_analytics_run(run_id=run_id, status="failed")
                    connection.commit()
                raise
    except FeatureStoreError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - CLI must fail without tracebacks.
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    action = "would write" if args.dry_run else "wrote"
    print(
        "OK: "
        f"{action} {summary.values_written} "
        f"{MOMENTUM_12_1_DEFINITION.name} value(s) for "
        f"{summary.securities_seen} security(ies); "
        f"eligible pairs={summary.eligible_security_dates}; "
        f"skipped insufficient_history={summary.skipped_insufficient_history}, "
        f"missing_price={summary.skipped_missing_price}"
    )
    return 0


def _date_arg(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("dates must use YYYY-MM-DD") from exc


def _validate_date_bounds(*, start_date: date | None, end_date: date | None) -> None:
    if start_date is not None and end_date is not None and end_date < start_date:
        raise FeatureStoreError("end_date must be on or after start_date")


def _load_psycopg() -> object:
    try:
        import psycopg
    except ImportError as exc:
        raise FeatureStoreError(
            "psycopg is required for live materialization; "
            "install the project dependencies first"
        ) from exc
    return psycopg


def _code_git_sha() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise FeatureStoreError("could not determine code_git_sha with git rev-parse")
    return result.stdout.strip()


if __name__ == "__main__":
    raise SystemExit(main())
