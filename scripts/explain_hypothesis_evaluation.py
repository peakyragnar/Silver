#!/usr/bin/env python
"""Explain a persisted hypothesis evaluation or falsifier backtest."""

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

from silver.analytics.hypothesis_evaluation_explainer import (  # noqa: E402
    HypothesisEvaluationExplanationError,
    load_hypothesis_evaluation_explanation,
    render_hypothesis_evaluation_explanation,
)


class ExplainHypothesisEvaluationError(RuntimeError):
    """Raised when the explainer CLI cannot complete."""


class PsqlJsonClient:
    """Tiny psql-backed JSON reader for persisted hypothesis evidence."""

    def __init__(self, *, database_url: str, psql_path: str | None = None) -> None:
        self._database_url = database_url
        self._psql_path = psql_path or shutil.which("psql")
        if self._psql_path is None:
            raise ExplainHypothesisEvaluationError(
                "psql is required to read hypothesis evaluation evidence"
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
            raise ExplainHypothesisEvaluationError(
                "psql failed while reading hypothesis evaluation evidence"
                f"{detail}. If the schema is missing, run "
                "`python scripts/bootstrap_database.py` first."
            )
        output = result.stdout.strip()
        if not output:
            raise ExplainHypothesisEvaluationError("psql returned no JSON output")
        try:
            return json.loads(output)
        except json.JSONDecodeError as exc:
            raise ExplainHypothesisEvaluationError("psql returned invalid JSON") from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    identity = parser.add_mutually_exclusive_group(required=True)
    identity.add_argument(
        "--backtest-run-id",
        type=int,
        help="durable backtest_runs.id to explain",
    )
    identity.add_argument(
        "--hypothesis-key",
        help="hypothesis key whose latest evaluation should be explained",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=5,
        help="number of strongest/weakest windows and tickers to show",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        help="optional markdown path; defaults to stdout only",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL; defaults to DATABASE_URL",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate CLI arguments without connecting to Postgres",
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
            print("OK: hypothesis evaluation explainer check passed")
            return 0
        run_explainer(args)
    except (
        ExplainHypothesisEvaluationError,
        HypothesisEvaluationExplanationError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


def run_explainer(args: argparse.Namespace) -> None:
    if not args.database_url:
        raise ExplainHypothesisEvaluationError(
            "DATABASE_URL is required unless --check is used. Run "
            "`python scripts/bootstrap_database.py` after setting DATABASE_URL, "
            "then rerun the explainer command."
        )

    client = PsqlJsonClient(database_url=args.database_url, psql_path=args.psql_path)
    explanation = load_hypothesis_evaluation_explanation(
        client,
        backtest_run_id=args.backtest_run_id,
        hypothesis_key=args.hypothesis_key,
    )
    rendered = render_hypothesis_evaluation_explanation(
        explanation,
        top=args.top,
    )
    if args.output_path is not None:
        report_path = _resolve_repo_path(args.output_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(rendered, encoding="utf-8")
        print(f"OK: wrote {_display_path(report_path)}")
        return
    print(rendered, end="")


def _validate_args(args: argparse.Namespace) -> None:
    if args.backtest_run_id is not None and args.backtest_run_id <= 0:
        raise ExplainHypothesisEvaluationError("backtest_run_id must be positive")
    if args.hypothesis_key is not None and not args.hypothesis_key.strip():
        raise ExplainHypothesisEvaluationError("hypothesis_key must be non-empty")
    if args.top <= 0:
        raise ExplainHypothesisEvaluationError("top must be positive")


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
