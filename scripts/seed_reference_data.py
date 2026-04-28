#!/usr/bin/env python
"""Check and seed Silver reference securities and universe membership."""

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

from silver.reference.seed_data import (  # noqa: E402
    DEFAULT_CONFIG_PATH,
    FALSIFIER_UNIVERSE_NAME,
    SeedApplyError,
    SeedValidationError,
    load_seed_file,
    seed_reference_data,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate the seed config without connecting to Postgres",
    )
    parser.add_argument(
        "--config-path",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="path to reference seed config",
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
        seed_config = load_seed_file(args.config_path)
        if args.check:
            print(
                "OK: "
                f"{len(seed_config.securities)} security seed(s), "
                f"{len(seed_config.falsifier_tickers)} "
                f"{FALSIFIER_UNIVERSE_NAME} membership seed(s) checked"
            )
            return 0
        if not args.database_url:
            raise SeedApplyError("DATABASE_URL is required unless --check is used")
        seed_reference_data(
            seed_config,
            args.database_url,
            psql_path=args.psql_path,
        )
    except (SeedValidationError, SeedApplyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        "OK: seeded "
        f"{len(seed_config.securities)} securities and "
        f"{len(seed_config.falsifier_tickers)} "
        f"{FALSIFIER_UNIVERSE_NAME} memberships"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
