#!/usr/bin/env python
"""Manage Silver hypothesis candidates and evaluation links."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence

from silver.hypotheses import (
    HypothesisRegistryError,
    HypothesisRepository,
    HypothesisSummary,
    momentum_12_1_hypothesis,
)

try:
    import psycopg
except ImportError:  # pragma: no cover - exercised through CLI error text.
    psycopg = None  # type: ignore[assignment]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL; defaults to DATABASE_URL",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("check", help="validate local CLI configuration")
    subparsers.add_parser(
        "seed-momentum",
        help="upsert the Phase 1 momentum_12_1 seed hypothesis",
    )

    record = subparsers.add_parser(
        "record-backtest",
        help="link a hypothesis to a completed backtest_run_id",
    )
    record.add_argument("--hypothesis-key", default="momentum_12_1")
    record.add_argument("--backtest-run-id", type=int, required=True)
    record.add_argument(
        "--status",
        choices=("running", "rejected", "promising", "accepted", "failed"),
        help="override inferred evaluation status",
    )
    record.add_argument("--failure-reason")
    record.add_argument("--notes")

    latest = subparsers.add_parser(
        "record-latest-falsifier",
        help="link a hypothesis to the latest succeeded falsifier backtest",
    )
    latest.add_argument("--hypothesis-key", default="momentum_12_1")
    latest.add_argument("--strategy", default="momentum_12_1")
    latest.add_argument("--universe", default="falsifier_seed")
    latest.add_argument("--horizon", type=int, default=63)
    latest.add_argument(
        "--status",
        choices=("running", "rejected", "promising", "accepted", "failed"),
        help="override inferred evaluation status",
    )
    latest.add_argument("--failure-reason")
    latest.add_argument("--notes")

    subparsers.add_parser("list", help="list hypotheses and latest evaluations")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.command == "check":
            run_check(args)
            return 0

        with _connect_database(args.database_url) as connection:
            repository = HypothesisRepository(connection)
            if args.command == "seed-momentum":
                record = repository.upsert_hypothesis(momentum_12_1_hypothesis())
                print(f"OK: seeded hypothesis {record.hypothesis_key} ({record.name})")
                return 0
            if args.command == "record-backtest":
                evaluation = repository.record_backtest_evaluation(
                    hypothesis_key=args.hypothesis_key,
                    backtest_run_id=args.backtest_run_id,
                    evaluation_status=args.status,
                    failure_reason=args.failure_reason,
                    notes=args.notes,
                )
                print(
                    "OK: recorded evaluation "
                    f"{evaluation.evaluation_status} for backtest_run_id "
                    f"{evaluation.backtest_run_id}"
                )
                return 0
            if args.command == "record-latest-falsifier":
                evaluation = repository.record_latest_falsifier_evaluation(
                    hypothesis_key=args.hypothesis_key,
                    strategy=args.strategy,
                    universe_name=args.universe,
                    horizon_days=args.horizon,
                    evaluation_status=args.status,
                    failure_reason=args.failure_reason,
                    notes=args.notes,
                )
                print(
                    "OK: recorded latest falsifier evaluation "
                    f"{evaluation.evaluation_status} for backtest_run_id "
                    f"{evaluation.backtest_run_id}"
                )
                return 0
            if args.command == "list":
                print(render_hypothesis_summaries(repository.list_hypotheses()))
                return 0
    except HypothesisRegistryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"error: unsupported command {args.command}", file=sys.stderr)
    return 1


def run_check(args: argparse.Namespace) -> None:
    if psycopg is None:
        raise HypothesisRegistryError(
            "psycopg is required for hypothesis registry writes; run `uv sync`"
        )
    if not args.database_url:
        raise HypothesisRegistryError(
            "DATABASE_URL is required unless only inspecting --help"
        )
    print("OK: hypothesis registry CLI check passed")


def render_hypothesis_summaries(rows: Sequence[HypothesisSummary]) -> str:
    if not rows:
        return "No hypotheses recorded."
    lines = [
        "hypothesis_key | status | latest_eval | backtest_run_id | backtest_run_key",
        "--- | --- | --- | --- | ---",
    ]
    for row in rows:
        lines.append(
            " | ".join(
                (
                    row.hypothesis_key,
                    row.status,
                    row.latest_evaluation_status or "-",
                    "-" if row.latest_backtest_run_id is None else str(row.latest_backtest_run_id),
                    row.latest_backtest_run_key or "-",
                )
            )
        )
    return "\n".join(lines)


def _connect_database(database_url: str | None):
    if psycopg is None:
        raise HypothesisRegistryError(
            "psycopg is required for hypothesis registry writes; run `uv sync`"
        )
    if not database_url:
        raise HypothesisRegistryError("DATABASE_URL is required")
    return psycopg.connect(database_url)


if __name__ == "__main__":
    raise SystemExit(main())
