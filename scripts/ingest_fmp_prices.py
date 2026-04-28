#!/usr/bin/env python
"""Ingest FMP historical daily prices for a persisted Silver universe."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections.abc import Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from silver.ingest import FmpPriceIngestError, RawVault, ingest_fmp_prices  # noqa: E402
from silver.reference.seed_data import (  # noqa: E402
    DEFAULT_CONFIG_PATH,
    FALSIFIER_UNIVERSE_NAME,
    SeedValidationError,
    load_seed_file,
)
from silver.sources.fmp import FMPClient, FMPClientError  # noqa: E402


class CommandError(RuntimeError):
    """Raised for CLI-level configuration and connection failures."""


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--universe",
        default=FALSIFIER_UNIVERSE_NAME,
        help=f"universe name to ingest; defaults to {FALSIFIER_UNIVERSE_NAME}",
    )
    parser.add_argument(
        "--start-date",
        type=_date_arg,
        help="inclusive start date in YYYY-MM-DD format; defaults to universe start",
    )
    parser.add_argument(
        "--end-date",
        type=_date_arg,
        help="inclusive end date in YYYY-MM-DD format; defaults to today",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL; defaults to DATABASE_URL",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="read persisted universe membership and print the planned ingest",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate local seed config and CLI dates without DB or FMP access",
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
                start_date=args.start_date,
                end_date=args.end_date,
                seed_config_path=args.seed_config_path,
            )
            print(result)
            return 0

        if not args.database_url:
            raise CommandError(
                "DATABASE_URL is required; pass --database-url or set DATABASE_URL"
            )
        if not args.dry_run and not os.environ.get("FMP_API_KEY"):
            raise CommandError(
                "FMP_API_KEY is required unless --check or --dry-run is used"
            )

        connection = connect_database(args.database_url)
        try:
            client = None
            if not args.dry_run:
                client = FMPClient(raw_vault=RawVault(connection))
            result = ingest_fmp_prices(
                connection=connection,
                client=client,
                universe=args.universe,
                start_date=args.start_date,
                end_date=args.end_date,
                code_git_sha=code_git_sha(),
                dry_run=args.dry_run,
            )
        finally:
            close = getattr(connection, "close", None)
            if close is not None:
                close()
    except (
        CommandError,
        FmpPriceIngestError,
        FMPClientError,
        SeedValidationError,
    ) as exc:
        print(f"error: {redact(str(exc), args.database_url)}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - CLI must fail without tracebacks/secrets.
        print(f"error: {redact(str(exc), args.database_url)}", file=sys.stderr)
        return 1

    print(format_result(result))
    return 0


def check_config(
    *,
    universe: str,
    start_date: date | None,
    end_date: date | None,
    seed_config_path: Path,
    today: date | None = None,
) -> str:
    seed_config = load_seed_file(seed_config_path)
    memberships = tuple(
        membership
        for membership in seed_config.universe_memberships
        if membership.universe_name == universe
    )
    if not memberships:
        raise CommandError(f"seed config has no memberships for universe {universe}")
    resolved_start = start_date or min(
        membership.valid_from for membership in memberships
    )
    resolved_end = end_date or today or datetime.now(timezone.utc).date()
    if resolved_start > resolved_end:
        raise CommandError("start_date must be on or before end_date")
    tickers = tuple(sorted({membership.ticker for membership in memberships}))
    return (
        "OK: checked FMP price ingest config for "
        f"{universe} {resolved_start.isoformat()}..{resolved_end.isoformat()} "
        f"with {len(tickers)} seed ticker(s): {', '.join(tickers)}"
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
        f"OK: {mode} FMP prices for {result.universe} "
        f"{result.start_date.isoformat()}..{result.end_date.isoformat()} "
        f"with {len(result.tickers)} ticker(s): {', '.join(result.tickers)}"
    )
    if result.dry_run:
        return summary
    return (
        f"{summary}; raw_responses={result.raw_responses_captured}, "
        f"rows_written={result.rows_written}, run_id={result.run_id}"
    )


def redact(message: str, database_url: str | None) -> str:
    redacted = message
    api_key = os.environ.get("FMP_API_KEY")
    for secret in (database_url, api_key):
        if secret:
            redacted = redacted.replace(secret, "[REDACTED]")
    return redacted


def _date_arg(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "must be an ISO date in YYYY-MM-DD format"
        ) from exc


if __name__ == "__main__":
    raise SystemExit(main())
