#!/usr/bin/env python
"""Validate local Phase 1 prerequisites without touching live services."""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


ROOT = Path(__file__).resolve().parents[1]

CheckStatus = Literal["ok", "warning", "error"]
FindSpec = Callable[[str], object | None]


@dataclass(frozen=True, slots=True)
class CheckResult:
    status: CheckStatus
    subject: str
    message: str


@dataclass(frozen=True, slots=True)
class ExpectedPath:
    relative_path: str
    kind: Literal["file", "dir"]


REQUIRED_COMMANDS: tuple[str, ...] = ("psql",)
REQUIRED_IMPORTS: tuple[str, ...] = (
    "silver",
    "psycopg",
    "yaml",
    "pandas_market_calendars",
)
EXPECTED_REPO_PATHS: tuple[ExpectedPath, ...] = (
    ExpectedPath("pyproject.toml", "file"),
    ExpectedPath("config/available_at_policies.yaml", "file"),
    ExpectedPath("config/seed_reference_data.yaml", "file"),
    ExpectedPath("config/trading_calendar.yaml", "file"),
    ExpectedPath("db/migrations", "dir"),
    ExpectedPath("db/seed/trading_calendar.csv", "file"),
    ExpectedPath("scripts/bootstrap_database.py", "file"),
    ExpectedPath("scripts/ingest_fmp_prices.py", "file"),
    ExpectedPath("scripts/materialize_forward_labels.py", "file"),
    ExpectedPath("scripts/materialize_momentum_12_1.py", "file"),
    ExpectedPath("scripts/run_falsifier.py", "file"),
    ExpectedPath("reports/falsifier", "dir"),
    ExpectedPath("docs/PHASE1_RUNBOOK.md", "file"),
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "validate local commands, imports, environment variables, and repo "
            "paths without connecting to Postgres or FMP"
        ),
    )
    return parser.parse_args(argv)


def collect_checks(
    *,
    root: Path = ROOT,
    env: Mapping[str, str] | None = None,
    required_commands: Sequence[str] = REQUIRED_COMMANDS,
    required_imports: Sequence[str] = REQUIRED_IMPORTS,
    expected_paths: Sequence[ExpectedPath] = EXPECTED_REPO_PATHS,
    find_spec: FindSpec = importlib.util.find_spec,
) -> tuple[CheckResult, ...]:
    environment = os.environ if env is None else env
    normalized_root = root.resolve()
    src_path = str(normalized_root / "src")
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    results: list[CheckResult] = [_check_python_version()]
    results.extend(_check_commands(required_commands, environment))
    results.extend(_check_imports(required_imports, find_spec))
    results.extend(_check_environment(environment))
    results.extend(_check_paths(normalized_root, expected_paths))
    return tuple(results)


def exit_code(results: Sequence[CheckResult]) -> int:
    return 1 if any(result.status == "error" for result in results) else 0


def format_results(results: Sequence[CheckResult]) -> str:
    labels = {"ok": "OK", "warning": "WARN", "error": "FAIL"}
    lines = ["Phase 1 environment check", ""]
    for result in results:
        lines.append(f"{labels[result.status]}: {result.subject}: {result.message}")

    ok_count = sum(result.status == "ok" for result in results)
    warning_count = sum(result.status == "warning" for result in results)
    error_count = sum(result.status == "error" for result in results)
    outcome = "passed" if error_count == 0 else "failed"
    lines.extend(
        (
            "",
            (
                f"Summary: {ok_count} ok, {warning_count} warning(s), "
                f"{error_count} error(s)"
            ),
            f"Result: Phase 1 environment check {outcome}",
        )
    )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parse_args(argv)
    results = collect_checks()
    output = format_results(results)
    if exit_code(results) == 0:
        print(output)
        return 0

    print(output, file=sys.stderr)
    return 1


def _check_python_version() -> CheckResult:
    version = sys.version_info
    display = f"{version.major}.{version.minor}.{version.micro}"
    if version < (3, 10):
        return CheckResult(
            "error",
            "python",
            f"{display} is too old; Silver requires Python 3.10 or newer",
        )
    return CheckResult("ok", "python", f"{display} satisfies Python >=3.10")


def _check_commands(
    commands: Sequence[str],
    env: Mapping[str, str],
) -> tuple[CheckResult, ...]:
    path = env.get("PATH", "")
    results: list[CheckResult] = []
    for command in commands:
        resolved = shutil.which(command, path=path)
        if resolved is None:
            results.append(
                CheckResult(
                    "error",
                    f"command {command}",
                    "missing from PATH; install the Postgres client tools",
                )
            )
        else:
            results.append(
                CheckResult("ok", f"command {command}", f"found at {resolved}")
            )
    return tuple(results)


def _check_imports(
    imports: Sequence[str],
    find_spec: FindSpec,
) -> tuple[CheckResult, ...]:
    results: list[CheckResult] = []
    for module_name in imports:
        if find_spec(module_name) is None:
            results.append(
                CheckResult(
                    "error",
                    f"Python import {module_name}",
                    "unavailable; install project dependencies before Phase 1 runs",
                )
            )
        else:
            results.append(
                CheckResult("ok", f"Python import {module_name}", "available")
            )
    return tuple(results)


def _check_environment(env: Mapping[str, str]) -> tuple[CheckResult, ...]:
    results = [
        _check_required_secret_presence(
            env,
            "DATABASE_URL",
            "required for DB bootstrap, ingest, materialization, and falsifier runs",
        ),
        _check_optional_secret_presence(
            env,
            "FMP_API_KEY",
            "optional for preflight; required only when ingesting live FMP prices",
        ),
    ]
    return tuple(results)


def _check_required_secret_presence(
    env: Mapping[str, str],
    name: str,
    detail: str,
) -> CheckResult:
    if env.get(name):
        return CheckResult("ok", f"environment {name}", "set; value hidden")
    return CheckResult("error", f"environment {name}", f"not set; {detail}")


def _check_optional_secret_presence(
    env: Mapping[str, str],
    name: str,
    detail: str,
) -> CheckResult:
    if env.get(name):
        return CheckResult("ok", f"environment {name}", "set; value hidden")
    return CheckResult("warning", f"environment {name}", f"not set; {detail}")


def _check_paths(
    root: Path,
    expected_paths: Sequence[ExpectedPath],
) -> tuple[CheckResult, ...]:
    results: list[CheckResult] = []
    for expected in expected_paths:
        path = root / expected.relative_path
        if expected.kind == "file":
            exists = path.is_file()
        else:
            exists = path.is_dir()

        if exists:
            results.append(
                CheckResult("ok", f"repo path {expected.relative_path}", "present")
            )
        else:
            results.append(
                CheckResult(
                    "error",
                    f"repo path {expected.relative_path}",
                    f"missing expected {expected.kind}",
                )
            )
    return tuple(results)


if __name__ == "__main__":
    raise SystemExit(main())
