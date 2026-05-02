#!/usr/bin/env python
"""Generate a markdown research results report from persisted registry rows."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Sequence


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from silver.analytics.research_results import (  # noqa: E402
    ResearchResultsError,
    load_research_results_report,
    render_research_results_report,
)
from silver.features import DEFAULT_CANDIDATE_CONFIG_PATH  # noqa: E402
from silver.features.candidate_pack import load_feature_candidates  # noqa: E402


DEFAULT_OUTPUT_PATH = ROOT / "reports" / "research" / "results_v0.md"


class ResearchResultsReportCliError(RuntimeError):
    """Raised when the research results report CLI cannot complete."""


class PsqlJsonClient:
    """Tiny psql-backed JSON reader for persisted research results."""

    def __init__(self, *, database_url: str, psql_path: str | None = None) -> None:
        self._database_url = database_url
        self._psql_path = psql_path or shutil.which("psql")
        if self._psql_path is None:
            raise ResearchResultsReportCliError(
                "psql is required to read persisted research results"
            )

    def fetch_json(self, sql: str) -> Any:
        result = subprocess.run(
            [
                self._psql_path,
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-q",
                "-t",
                "-A",
                "-d",
                self._database_url,
            ],
            input=sql,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            stderr = result.stderr.replace(self._database_url, "[DATABASE_URL]").strip()
            detail = f": {stderr}" if stderr else ""
            raise ResearchResultsReportCliError(
                "psql failed while reading research results"
                f"{detail}. If the schema is missing, run "
                "`python scripts/bootstrap_database.py` first."
            )
        output = result.stdout.strip()
        if not output:
            raise ResearchResultsReportCliError("psql returned no JSON output")
        try:
            return json.loads(output)
        except json.JSONDecodeError as exc:
            raise ResearchResultsReportCliError("psql returned invalid JSON") from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-path",
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="markdown report path",
    )
    parser.add_argument(
        "--candidate-config",
        type=Path,
        default=DEFAULT_CANDIDATE_CONFIG_PATH,
        help="YAML feature-candidate definition file",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL; defaults to DATABASE_URL",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate CLI/config/report path without connecting to Postgres",
    )
    parser.add_argument(
        "--psql-path",
        help="path to psql; defaults to the first psql on PATH",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        _validate_args(args)
        if args.check:
            run_check(args)
            return 0
        run_report(args)
    except (ResearchResultsReportCliError, ResearchResultsError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


def run_check(args: argparse.Namespace) -> None:
    candidates = load_feature_candidates(_resolve_candidate_config_path(args))
    report_path = _resolve_repo_path(args.output_path)
    _validate_output_path(report_path)
    print(
        "OK: research results report check passed for "
        f"{len(candidates)} candidate definition(s) -> {_display_path(report_path)}"
    )


def run_report(args: argparse.Namespace) -> None:
    if not args.database_url:
        raise ResearchResultsReportCliError(
            "DATABASE_URL is required unless --check is used. Run "
            "`python scripts/bootstrap_database.py` after setting DATABASE_URL, "
            "then rerun the research results report."
        )

    report_path = _resolve_repo_path(args.output_path)
    _validate_output_path(report_path)
    client = PsqlJsonClient(database_url=args.database_url, psql_path=args.psql_path)
    report = load_research_results_report(
        client,
        candidate_config_path=_resolve_candidate_config_path(args),
    )
    rendered = render_research_results_report(report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(rendered, encoding="utf-8")
    print(
        f"OK: wrote {_display_path(report_path)} with "
        f"{len(report.results)} hypothesis result(s)"
    )


def _validate_args(args: argparse.Namespace) -> None:
    _resolve_candidate_config_path(args)
    _validate_output_path(_resolve_repo_path(args.output_path))


def _resolve_candidate_config_path(args: argparse.Namespace) -> Path:
    path = args.candidate_config
    if not isinstance(path, Path):
        raise ResearchResultsReportCliError("candidate_config must be a path")
    resolved = _resolve_repo_path(path)
    if not resolved.exists():
        raise ResearchResultsReportCliError(
            f"candidate config does not exist: {_display_path(resolved)}"
        )
    return resolved


def _validate_output_path(path: Path) -> None:
    if path.exists() and path.is_dir():
        raise ResearchResultsReportCliError("output path must be a file path")
    if path.suffix.lower() not in {"", ".md"}:
        raise ResearchResultsReportCliError("output path must be a markdown path")


def _resolve_repo_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return ROOT / path


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
