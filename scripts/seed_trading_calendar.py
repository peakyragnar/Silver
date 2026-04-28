#!/usr/bin/env python
"""Check, generate, and seed the Silver US equity trading calendar."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from silver.time.trading_calendar import (  # noqa: E402
    DEFAULT_CONFIG_PATH,
    DEFAULT_SEED_PATH,
    TradingCalendarGenerationError,
    TradingCalendarSeedError,
    TradingCalendarValidationError,
    assert_seed_csv_matches,
    generate_trading_calendar,
    load_calendar_config,
    seed_trading_calendar,
    validate_complete_calendar,
    write_seed_csv,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate config, generation, and seed CSV without connecting to Postgres",
    )
    parser.add_argument(
        "--write-seed",
        action="store_true",
        help="write the deterministic generated seed CSV and exit",
    )
    parser.add_argument(
        "--config-path",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="path to trading-calendar config",
    )
    parser.add_argument(
        "--seed-path",
        type=Path,
        default=DEFAULT_SEED_PATH,
        help="path to trading-calendar seed CSV",
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
        config = load_calendar_config(args.config_path)
        rows = generate_trading_calendar(config)
        validate_complete_calendar(rows, config.start_date, config.end_date)

        if args.write_seed:
            write_seed_csv(rows, args.seed_path)
            print(f"OK: wrote {len(rows)} trading-calendar row(s) to {args.seed_path}")

        if args.check:
            assert_seed_csv_matches(rows, args.seed_path)
            print(
                "OK: "
                f"{len(rows)} trading-calendar row(s) checked for "
                f"{config.start_date.isoformat()} through {config.end_date.isoformat()}"
            )
            return 0

        if args.write_seed:
            return 0

        if not args.database_url:
            raise TradingCalendarSeedError("DATABASE_URL is required unless --check is used")
        seed_trading_calendar(rows, args.database_url, psql_path=args.psql_path)
    except (
        TradingCalendarValidationError,
        TradingCalendarGenerationError,
        TradingCalendarSeedError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"OK: seeded {len(rows)} trading-calendar row(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
