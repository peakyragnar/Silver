#!/usr/bin/env python
"""Materialize Silver's deterministic feature-candidate pack."""

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
    DEFAULT_CANDIDATE_CONFIG_PATH,
    FeatureCandidate,
    FeatureStoreError,
    FeatureStoreRepository,
    feature_candidates_for_keys,
    feature_definition_hash,
    materialize_feature_candidate,
)
from silver.reference.seed_data import FALSIFIER_UNIVERSE_NAME  # noqa: E402


QUARTERLY_FILING_POLICY_NAME = "sec_10q_filing"
QUARTERLY_FILING_POLICY_VERSION = 1
FUNDAMENTAL_MATERIALIZERS = {
    "revenue_growth_yoy",
    "gross_margin",
    "operating_margin",
    "net_margin",
    "diluted_shares_change_yoy",
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate candidate definitions without connecting to Postgres",
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
        "--candidate",
        action="append",
        help="candidate key to materialize; repeat to choose several",
    )
    parser.add_argument(
        "--candidate-config",
        type=Path,
        default=DEFAULT_CANDIDATE_CONFIG_PATH,
        help="YAML feature-candidate definition file",
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
        candidate_config_path = _resolve_candidate_config_path(args.candidate_config)
        candidates = feature_candidates_for_keys(
            args.candidate,
            config_path=candidate_config_path,
        )
        if args.check:
            for candidate in candidates:
                print(_candidate_check_line(candidate))
            print(f"OK: {len(candidates)} feature candidate definition(s) checked")
            return 0

        if not args.database_url:
            raise FeatureStoreError("DATABASE_URL is required unless --check is used")

        psycopg = _load_psycopg()
        summaries = []
        with psycopg.connect(args.database_url) as connection:
            repository = FeatureStoreRepository(connection)
            available_at_cutoff = datetime.now(timezone.utc)
            for candidate in candidates:
                summaries.append(
                    _materialize_one_candidate(
                        connection=connection,
                        repository=repository,
                        candidate=candidate,
                        universe=args.universe,
                        start_date=args.start_date,
                        end_date=args.end_date,
                        dry_run=args.dry_run,
                        available_at_cutoff=available_at_cutoff,
                    )
                )
    except FeatureStoreError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - CLI must fail without tracebacks.
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    action = "would write" if args.dry_run else "wrote"
    for summary in summaries:
        skipped = ", ".join(
            f"{reason}={count}" for reason, count in summary.skipped_by_reason.items()
        )
        skipped_text = skipped or "none"
        print(
            "OK: "
            f"{action} {summary.values_written} {summary.candidate_key} value(s) "
            f"for {summary.securities_seen} security(ies); "
            f"eligible pairs={summary.eligible_security_dates}; "
            f"skipped {skipped_text}"
        )
    return 0


def _materialize_one_candidate(
    *,
    connection: object,
    repository: FeatureStoreRepository,
    candidate: FeatureCandidate,
    universe: str,
    start_date: date | None,
    end_date: date | None,
    dry_run: bool,
    available_at_cutoff: datetime,
):
    if dry_run:
        run_id = 1
    else:
        definition_hash = feature_definition_hash(candidate.definition)
        run_id = repository.create_feature_generation_run(
            code_git_sha=_code_git_sha(),
            feature_set_hash=definition_hash,
            available_at_policy_versions=_available_at_policy_versions(candidate),
            parameters={
                "candidate_key": candidate.hypothesis_key,
                "feature": candidate.signal_name,
                "feature_version": candidate.definition.version,
                "materializer": candidate.materializer,
                "selection_direction": candidate.selection_direction,
                "universe": universe,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None,
                "available_at_cutoff": available_at_cutoff.isoformat(),
            },
        )
        connection.commit()  # type: ignore[attr-defined]

    try:
        summary = materialize_feature_candidate(
            repository,
            candidate,
            universe_name=universe,
            start_date=start_date,
            end_date=end_date,
            computed_by_run_id=run_id,
            dry_run=dry_run,
            available_at_cutoff=available_at_cutoff,
        )
        if not dry_run:
            repository.finish_analytics_run(run_id=run_id, status="succeeded")
            connection.commit()  # type: ignore[attr-defined]
        return summary
    except Exception:
        if not dry_run:
            connection.rollback()  # type: ignore[attr-defined]
            repository.finish_analytics_run(run_id=run_id, status="failed")
            connection.commit()  # type: ignore[attr-defined]
        raise


def _candidate_check_line(candidate: FeatureCandidate) -> str:
    return (
        "candidate="
        f"{candidate.hypothesis_key}; feature={candidate.signal_name} "
        f"v{candidate.definition.version}; "
        f"selection_direction={candidate.selection_direction}; "
        f"definition_hash={feature_definition_hash(candidate.definition)}"
    )


def _available_at_policy_versions(candidate: FeatureCandidate) -> dict[str, int]:
    if candidate.materializer in FUNDAMENTAL_MATERIALIZERS:
        return {QUARTERLY_FILING_POLICY_NAME: QUARTERLY_FILING_POLICY_VERSION}
    return {DAILY_PRICE_POLICY_NAME: DAILY_PRICE_POLICY_VERSION}


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


def _resolve_candidate_config_path(path: Path) -> Path:
    candidate_path = path.expanduser()
    if candidate_path.is_absolute():
        return candidate_path
    return (Path.cwd() / candidate_path).resolve()


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
