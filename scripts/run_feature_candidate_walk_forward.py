#!/usr/bin/env python
"""Run harder walk-forward evidence over configured feature candidates."""

from __future__ import annotations

import argparse
import math
import os
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SCRIPTS = ROOT / "scripts"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from silver.features import (  # noqa: E402
    DEFAULT_CANDIDATE_CONFIG_PATH,
    FeatureCandidate,
    FeatureStoreError,
    feature_candidates_for_keys,
)
from silver.hypotheses import (  # noqa: E402
    HypothesisRegistryError,
    HypothesisRepository,
)
from silver.reference.seed_data import FALSIFIER_UNIVERSE_NAME  # noqa: E402

from run_feature_candidate_pack import (  # noqa: E402
    candidate_hypothesis,
    parse_backtest_run_id,
)


DEFAULT_OUTPUT_DIR = ROOT / "reports" / "falsifier" / "candidate_walk_forward"
DEFAULT_MIN_SCORED_WINDOWS = 2
DEFAULT_MIN_POSITIVE_WINDOW_RATE = 0.6
BACKTEST_RUN_ID_RE = re.compile(r"\bbacktest_run_id=(?P<id>\d+)\b")


@dataclass(frozen=True, slots=True)
class WalkForwardEvidence:
    scored_windows: int
    positive_windows: int
    positive_window_rate: float | None
    mean_net_difference_vs_baseline: float | None
    walk_forward_status: str
    failure_reason: str | None


@dataclass(frozen=True, slots=True)
class BacktestEvidenceSource:
    backtest_run_id: int
    metrics: Mapping[str, object]
    label_scramble_pass: bool | None


@dataclass(frozen=True, slots=True)
class CandidateWalkForwardResult:
    candidate_key: str
    feature_name: str
    selection_direction: str
    backtest_run_id: int
    walk_forward_status: str
    scored_windows: int
    positive_windows: int
    positive_window_rate: float | None
    mean_net_difference_vs_baseline: float | None
    evaluation_status: str
    failure_reason: str | None
    report_path: Path


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate candidate and walk-forward configuration without Postgres",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL; defaults to DATABASE_URL",
    )
    parser.add_argument(
        "--universe",
        default=FALSIFIER_UNIVERSE_NAME,
        help=f"universe name to evaluate; defaults to {FALSIFIER_UNIVERSE_NAME}",
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=63,
        help="forward-return horizon in trading sessions; defaults to 63",
    )
    parser.add_argument(
        "--candidate",
        action="append",
        help="candidate key to run; repeat to choose several",
    )
    parser.add_argument(
        "--candidate-config",
        type=Path,
        default=DEFAULT_CANDIDATE_CONFIG_PATH,
        help="YAML feature-candidate definition file",
    )
    parser.add_argument(
        "--skip-materialize",
        action="store_true",
        help="evaluate existing feature_values without refreshing candidates first",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="directory for per-candidate falsifier reports",
    )
    parser.add_argument(
        "--min-scored-windows",
        type=int,
        default=DEFAULT_MIN_SCORED_WINDOWS,
        help="minimum walk-forward windows required for a passing evidence rollup",
    )
    parser.add_argument(
        "--min-positive-window-rate",
        type=float,
        default=DEFAULT_MIN_POSITIVE_WINDOW_RATE,
        help="minimum fraction of windows beating baseline; defaults to 0.6",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        _validate_thresholds(args)
        candidate_config_path = _resolve_candidate_config_path(args.candidate_config)
        candidates = feature_candidates_for_keys(
            args.candidate,
            config_path=candidate_config_path,
        )
        if args.check:
            print(
                "OK: feature candidate walk-forward check passed for "
                + ", ".join(candidate.hypothesis_key for candidate in candidates)
            )
            return 0
        if not args.database_url:
            raise FeatureStoreError("DATABASE_URL is required unless --check is used")

        psycopg = _load_psycopg()
        with psycopg.connect(args.database_url) as connection:
            repository = HypothesisRepository(connection)
            results = []
            for candidate in candidates:
                try:
                    results.append(
                        run_candidate_walk_forward(
                            candidate,
                            repository=repository,
                            connection=connection,
                            database_url=args.database_url,
                            universe=args.universe,
                            horizon=args.horizon,
                            output_dir=args.output_dir,
                            skip_materialize=args.skip_materialize,
                            candidate_config_path=candidate_config_path,
                            min_scored_windows=args.min_scored_windows,
                            min_positive_window_rate=args.min_positive_window_rate,
                        )
                    )
                except Exception:
                    connection.rollback()
                    raise
                connection.commit()
    except (FeatureStoreError, HypothesisRegistryError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - CLI should fail without traceback.
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print(render_walk_forward_results(results))
    return 0


def run_candidate_walk_forward(
    candidate: FeatureCandidate,
    *,
    repository: HypothesisRepository,
    connection: object,
    database_url: str,
    universe: str,
    horizon: int,
    output_dir: Path,
    skip_materialize: bool,
    candidate_config_path: Path,
    min_scored_windows: int,
    min_positive_window_rate: float,
) -> CandidateWalkForwardResult:
    if not skip_materialize:
        _run_command(
            _materialize_command(
                candidate,
                universe=universe,
                candidate_config_path=candidate_config_path,
            ),
            database_url=database_url,
        )

    repository.upsert_hypothesis(
        candidate_hypothesis(candidate, universe=universe, horizon=horizon)
    )
    report_path = output_dir / f"{candidate.hypothesis_key}_wf_h{horizon}.md"
    stdout = _run_command(
        _falsifier_command(
            candidate,
            universe=universe,
            horizon=horizon,
            output_path=report_path,
        ),
        database_url=database_url,
    )
    backtest_run_id = parse_backtest_run_id(stdout)
    source = load_backtest_evidence_source(connection, backtest_run_id)
    evidence = walk_forward_evidence_from_metrics(
        source.metrics,
        min_scored_windows=min_scored_windows,
        min_positive_window_rate=min_positive_window_rate,
    )
    explicit_status, explicit_failure = harder_evaluation_status(
        evidence,
        label_scramble_pass=source.label_scramble_pass,
    )
    evaluation = repository.record_backtest_evaluation(
        hypothesis_key=candidate.hypothesis_key,
        backtest_run_id=backtest_run_id,
        evaluation_status=explicit_status,
        failure_reason=explicit_failure,
        notes=(
            "feature candidate walk-forward v1 evaluation; "
            f"walk_forward_status={evidence.walk_forward_status}"
        ),
    )
    return CandidateWalkForwardResult(
        candidate_key=candidate.hypothesis_key,
        feature_name=candidate.signal_name,
        selection_direction=candidate.selection_direction,
        backtest_run_id=evaluation.backtest_run_id,
        walk_forward_status=evidence.walk_forward_status,
        scored_windows=evidence.scored_windows,
        positive_windows=evidence.positive_windows,
        positive_window_rate=evidence.positive_window_rate,
        mean_net_difference_vs_baseline=evidence.mean_net_difference_vs_baseline,
        evaluation_status=evaluation.evaluation_status,
        failure_reason=evaluation.failure_reason,
        report_path=report_path,
    )


def walk_forward_evidence_from_metrics(
    metrics: Mapping[str, object],
    *,
    min_scored_windows: int,
    min_positive_window_rate: float,
) -> WalkForwardEvidence:
    _validate_evidence_thresholds(
        min_scored_windows=min_scored_windows,
        min_positive_window_rate=min_positive_window_rate,
    )
    windows = metrics.get("walk_forward_windows")
    if not isinstance(windows, list):
        raise FeatureStoreError(
            "backtest metrics missing walk_forward_windows; rerun the falsifier"
        )
    differences = tuple(_window_net_difference(window) for window in windows)
    scored_windows = len(differences)
    positive_windows = sum(1 for value in differences if value > 0)
    positive_rate = (
        positive_windows / scored_windows if scored_windows else None
    )
    mean_difference = _mean(differences)

    if scored_windows < min_scored_windows:
        return WalkForwardEvidence(
            scored_windows=scored_windows,
            positive_windows=positive_windows,
            positive_window_rate=positive_rate,
            mean_net_difference_vs_baseline=mean_difference,
            walk_forward_status="insufficient_data",
            failure_reason="insufficient_walk_forward_windows",
        )
    if (
        positive_rate is None
        or positive_rate < min_positive_window_rate
        or mean_difference is None
        or mean_difference <= 0
    ):
        return WalkForwardEvidence(
            scored_windows=scored_windows,
            positive_windows=positive_windows,
            positive_window_rate=positive_rate,
            mean_net_difference_vs_baseline=mean_difference,
            walk_forward_status="failed",
            failure_reason="walk_forward_unstable",
        )
    return WalkForwardEvidence(
        scored_windows=scored_windows,
        positive_windows=positive_windows,
        positive_window_rate=positive_rate,
        mean_net_difference_vs_baseline=mean_difference,
        walk_forward_status="passed",
        failure_reason=None,
    )


def harder_evaluation_status(
    evidence: WalkForwardEvidence,
    *,
    label_scramble_pass: bool | None,
) -> tuple[str | None, str | None]:
    if evidence.failure_reason is not None:
        return "rejected", evidence.failure_reason
    if label_scramble_pass is False:
        return "rejected", "label_scramble_failed"
    if label_scramble_pass is None:
        return "failed", "label_scramble_missing"
    return None, None


def load_backtest_evidence_source(
    connection: object,
    backtest_run_id: int,
) -> BacktestEvidenceSource:
    with connection.cursor() as cursor:  # type: ignore[attr-defined]
        cursor.execute(
            """
            SELECT metrics, label_scramble_pass
            FROM silver.backtest_runs
            WHERE id = %s
            """,
            (backtest_run_id,),
        )
        row = cursor.fetchone()
    if row is None:
        raise FeatureStoreError(f"backtest_run_id {backtest_run_id} was not found")
    metrics = row[0]
    if not isinstance(metrics, Mapping):
        raise FeatureStoreError("backtest metrics must be a JSON object")
    return BacktestEvidenceSource(
        backtest_run_id=backtest_run_id,
        metrics=metrics,
        label_scramble_pass=row[1],
    )


def render_walk_forward_results(
    results: Sequence[CandidateWalkForwardResult],
) -> str:
    if not results:
        return "No feature candidates evaluated."
    lines = [
        "candidate | feature | direction | wf_status | positive_windows | "
        "positive_rate | mean_diff | verdict | backtest_run_id | report",
        "--- | --- | --- | --- | --- | --- | --- | --- | --- | ---",
    ]
    for result in results:
        verdict = result.evaluation_status
        if result.failure_reason:
            verdict = f"{verdict} ({result.failure_reason})"
        lines.append(
            " | ".join(
                (
                    result.candidate_key,
                    result.feature_name,
                    result.selection_direction,
                    result.walk_forward_status,
                    f"{result.positive_windows}/{result.scored_windows}",
                    _rate(result.positive_window_rate),
                    _percent(result.mean_net_difference_vs_baseline),
                    verdict,
                    str(result.backtest_run_id),
                    _display_path(result.report_path),
                )
            )
        )
    return "\n".join(lines)


def _materialize_command(
    candidate: FeatureCandidate,
    *,
    universe: str,
    candidate_config_path: Path,
) -> list[str]:
    return [
        sys.executable,
        str(ROOT / "scripts" / "materialize_feature_candidates.py"),
        "--universe",
        universe,
        "--candidate-config",
        str(candidate_config_path),
        "--candidate",
        candidate.hypothesis_key,
    ]


def _falsifier_command(
    candidate: FeatureCandidate,
    *,
    universe: str,
    horizon: int,
    output_path: Path,
) -> list[str]:
    command = [
        sys.executable,
        str(ROOT / "scripts" / "run_falsifier.py"),
        "--strategy",
        candidate.signal_name,
        "--horizon",
        str(horizon),
        "--universe",
        universe,
        "--output-path",
        str(output_path),
    ]
    if candidate.selection_direction != "high":
        command.extend(["--selection-direction", candidate.selection_direction])
    return command


def _run_command(command: Sequence[str], *, database_url: str) -> str:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    result = subprocess.run(
        list(command),
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        detail = stderr or stdout or f"exit code {result.returncode}"
        raise FeatureStoreError(f"candidate walk-forward command failed: {detail}")
    return result.stdout


def _window_net_difference(window: object) -> float:
    if not isinstance(window, Mapping):
        raise FeatureStoreError("walk_forward_windows entries must be JSON objects")
    value = window.get("net_difference_vs_baseline")
    if value is None:
        strategy = window.get("strategy_net_return")
        baseline = window.get("baseline_net_return")
        if not isinstance(strategy, (int, float)) or not isinstance(
            baseline,
            (int, float),
        ):
            raise FeatureStoreError(
                "walk-forward window missing net-difference fields"
            )
        value = float(strategy) - float(baseline)
    if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise FeatureStoreError("walk-forward net difference must be finite")
    return float(value)


def _validate_thresholds(args: argparse.Namespace) -> None:
    _validate_evidence_thresholds(
        min_scored_windows=args.min_scored_windows,
        min_positive_window_rate=args.min_positive_window_rate,
    )


def _validate_evidence_thresholds(
    *,
    min_scored_windows: int,
    min_positive_window_rate: float,
) -> None:
    if (
        isinstance(min_scored_windows, bool)
        or not isinstance(min_scored_windows, int)
        or min_scored_windows < 1
    ):
        raise FeatureStoreError("min_scored_windows must be a positive integer")
    if (
        isinstance(min_positive_window_rate, bool)
        or not isinstance(min_positive_window_rate, (int, float))
        or not math.isfinite(float(min_positive_window_rate))
        or not 0 <= min_positive_window_rate <= 1
    ):
        raise FeatureStoreError(
            "min_positive_window_rate must be between 0 and 1 inclusive"
        )


def _mean(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.3f}"


def _percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4%}"


def _load_psycopg() -> object:
    try:
        import psycopg
    except ImportError as exc:
        raise FeatureStoreError(
            "psycopg is required for candidate walk-forward evaluation; run `uv sync`"
        ) from exc
    return psycopg


def _resolve_candidate_config_path(path: Path) -> Path:
    candidate_path = path.expanduser()
    if candidate_path.is_absolute():
        return candidate_path
    return (Path.cwd() / candidate_path).resolve()


def _display_path(path: Path) -> str:
    resolved = path if path.is_absolute() else ROOT / path
    try:
        return str(resolved.resolve().relative_to(ROOT))
    except ValueError:
        return str(resolved)


if __name__ == "__main__":
    raise SystemExit(main())
