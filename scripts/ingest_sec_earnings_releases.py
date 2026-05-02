#!/usr/bin/env python
"""Ingest SEC 8-K Item 2.02 earnings release events."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from silver.ingest import (  # noqa: E402
    RawVault,
    SecEarningsReleaseIngestError,
    ingest_sec_earnings_releases,
)
from silver.ingest.sec_earnings_releases import (  # noqa: E402
    DEFAULT_CANDIDATE_LIMIT,
    DEFAULT_SINCE_DATE,
)
from silver.reference.seed_data import (  # noqa: E402
    DEFAULT_CONFIG_PATH,
    FALSIFIER_UNIVERSE_NAME,
    SeedValidationError,
    load_seed_file,
)
from silver.sources.sec import SECClient, SECClientError  # noqa: E402


class CommandError(RuntimeError):
    """Raised for CLI-level configuration failures."""


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--universe",
        default=FALSIFIER_UNIVERSE_NAME,
        help=f"universe name to ingest; defaults to {FALSIFIER_UNIVERSE_NAME}",
    )
    parser.add_argument(
        "--ticker",
        action="append",
        dest="tickers",
        help="ticker to ingest; may be repeated; defaults to all universe tickers",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int_arg,
        help="maximum number of selected tickers to ingest",
    )
    parser.add_argument(
        "--since-date",
        type=_date_arg,
        default=DEFAULT_SINCE_DATE,
        help=f"first SEC filing date to consider; default {DEFAULT_SINCE_DATE}",
    )
    parser.add_argument(
        "--candidate-limit",
        type=_positive_int_arg,
        default=DEFAULT_CANDIDATE_LIMIT,
        help=(
            "maximum Item 2.02 candidates to inspect per ticker; "
            f"default {DEFAULT_CANDIDATE_LIMIT}"
        ),
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL; defaults to DATABASE_URL",
    )
    parser.add_argument(
        "--sec-user-agent",
        default=os.environ.get("SEC_USER_AGENT"),
        help="SEC User-Agent header; defaults to SEC_USER_AGENT",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.2,
        help="pause between tickers; default is 0.2 seconds",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="read persisted universe membership and print the planned ingest",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate local seed config and CLI selection without DB or SEC access",
    )
    parser.add_argument(
        "--seed-config-path",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="path to reference seed config for --check",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.check:
            result = check_config(
                universe=args.universe,
                tickers=args.tickers,
                limit=args.limit,
                candidate_limit=args.candidate_limit,
                seed_config_path=args.seed_config_path,
            )
            print(result)
            return 0

        if not args.database_url:
            raise CommandError(
                "DATABASE_URL is required; pass --database-url or set DATABASE_URL"
            )
        if not args.dry_run and not args.sec_user_agent:
            raise CommandError(
                "SEC_USER_AGENT is required unless --check or --dry-run is used"
            )

        connection = connect_database(args.database_url)
        try:
            client = None
            if not args.dry_run:
                client = SECClient(
                    raw_vault=RawVault(connection),
                    user_agent=args.sec_user_agent,
                )
            result = ingest_sec_earnings_releases(
                connection=connection,
                client=client,
                universe=args.universe,
                tickers=args.tickers,
                limit=args.limit,
                since_date=args.since_date,
                candidate_limit=args.candidate_limit,
                code_git_sha=code_git_sha(),
                dry_run=args.dry_run,
                sleep_seconds=args.sleep_seconds,
            )
        finally:
            close = getattr(connection, "close", None)
            if close is not None:
                close()
    except (
        CommandError,
        SecEarningsReleaseIngestError,
        SECClientError,
        SeedValidationError,
    ) as exc:
        print(
            f"error: {redact(str(exc), args.database_url, args.sec_user_agent)}",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001 - CLI must fail without traces/secrets.
        print(
            f"error: {redact(str(exc), args.database_url, args.sec_user_agent)}",
            file=sys.stderr,
        )
        return 1

    print(format_result(result))
    return 0


def check_config(
    *,
    universe: str,
    tickers: Sequence[str] | None,
    limit: int | None,
    candidate_limit: int,
    seed_config_path: Path,
) -> str:
    seed_config = load_seed_file(seed_config_path)
    memberships = tuple(
        membership
        for membership in seed_config.universe_memberships
        if membership.universe_name == universe
    )
    if not memberships:
        raise CommandError(f"seed config has no memberships for universe {universe}")
    selected_tickers = tuple(sorted({membership.ticker for membership in memberships}))
    if tickers:
        requested = tuple(sorted({_ticker(ticker) for ticker in tickers}))
        missing = set(requested) - set(selected_tickers)
        if missing:
            raise CommandError(
                f"selected ticker(s) are not in seed universe: "
                f"{', '.join(sorted(missing))}"
            )
        selected_tickers = requested
    if limit is not None:
        selected_tickers = selected_tickers[:limit]
    return (
        "OK: checked SEC earnings release ingest config for "
        f"{universe} with {len(selected_tickers)} seed ticker(s), "
        f"candidate_limit={candidate_limit}"
    )


def connect_database(database_url: str) -> Any:
    try:
        import psycopg  # type: ignore[import-not-found]
    except ImportError as exc:
        raise CommandError(
            "psycopg is required to connect to Postgres; run uv sync"
        ) from exc

    try:
        return psycopg.connect(database_url)
    except Exception as exc:  # noqa: BLE001 - sanitize DB adapter details.
        raise CommandError(
            f"could not connect to Postgres: {type(exc).__name__}"
        ) from exc


def code_git_sha() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    return "unknown"


def format_result(result: Any) -> str:
    mode = "dry run" if result.dry_run else "ingested"
    summary = (
        f"OK: {mode} SEC earnings releases for {result.universe} "
        f"with {len(result.tickers)} ticker(s): {', '.join(result.tickers)}; "
        f"since_date={result.since_date}; candidate_limit={result.candidate_limit}"
    )
    if result.dry_run:
        return summary
    return (
        f"{summary}; events_written={result.events_written}, "
        f"linked_income_fundamental_rows={result.linked_income_fundamental_rows}, "
        f"run_id={result.run_id}"
    )


def redact(message: str, database_url: str | None, sec_user_agent: str | None) -> str:
    redacted = message
    for secret in (database_url, sec_user_agent):
        if secret:
            redacted = redacted.replace(secret, "[REDACTED]")
    return redacted


def _ticker(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CommandError("ticker must be a non-empty string")
    return value.strip().upper()


def _positive_int_arg(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _date_arg(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be YYYY-MM-DD") from exc


if __name__ == "__main__":
    raise SystemExit(main())
