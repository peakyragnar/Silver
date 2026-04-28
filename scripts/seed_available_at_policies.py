#!/usr/bin/env python
"""Check and seed Silver available_at policy definitions."""

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

from silver.time.available_at_policies import (  # noqa: E402
    DEFAULT_CONFIG_PATH,
    PolicySeedError,
    PolicyValidationError,
    load_policy_file,
    seed_policies,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate the policy config without connecting to Postgres",
    )
    parser.add_argument(
        "--config-path",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="path to available_at policy config",
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
        policies = load_policy_file(args.config_path)
        if args.check:
            print(f"OK: {len(policies)} available_at policy definition(s) checked")
            return 0
        if not args.database_url:
            raise PolicySeedError("DATABASE_URL is required unless --check is used")
        seed_policies(policies, args.database_url, psql_path=args.psql_path)
    except (PolicyValidationError, PolicySeedError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"OK: seeded {len(policies)} available_at policy definition(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
