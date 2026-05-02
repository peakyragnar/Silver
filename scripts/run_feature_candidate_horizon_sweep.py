#!/usr/bin/env python
"""Run configured feature candidates across canonical return horizons."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections import OrderedDict
from collections.abc import Sequence
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
    HypothesisCreate,
    HypothesisRegistryError,
    HypothesisRepository,
)
from silver.reference.seed_data import FALSIFIER_UNIVERSE_NAME  # noqa: E402
from silver.time.trading_calendar import CANONICAL_HORIZONS  # noqa: E402

from run_feature_candidate_pack import parse_backtest_run_id  # noqa: E402
from run_feature_candidate_walk_forward import (  # noqa: E402
    DEFAULT_MIN_POSITIVE_WINDOW_RATE,
    DEFAULT_MIN_SCORED_WINDOWS,
    harder_evaluation_status,
    load_backtest_evidence_source,
    walk_forward_evidence_from_metrics,
)


DEFAULT_OUTPUT_DIR = ROOT / "reports" / "falsifier" / "horizon_sweep"
DEFAULT_BASE_HORIZON = 63


@dataclass(frozen=True, slots=True)
class ExistingHorizonEvaluation:
    hypothesis_key: str
    backtest_run_id: int
    evaluation_status: str
    failure_reason: str | None


@dataclass(frozen=True, slots=True)
class HorizonSweepResult:
    candidate_key: str
    hypothesis_key: str
    feature_name: str
    selection_direction: str
    horizon: int
    action: str
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
        help="validate candidate and horizon configuration without Postgres",
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
        "--candidate",
        action="append",
        help="candidate key to sweep; repeat to choose several",
    )
    parser.add_argument(
        "--horizon",
        action="append",
        type=int,
        help="canonical horizon to run; repeat to choose several",
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
        "--rerun-existing",
        action="store_true",
        help="rerun cells that already have a linked hypothesis evaluation",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="directory for per-cell falsifier reports",
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
        horizons = selected_horizons(args.horizon)
        _validate_evidence_thresholds(
            min_scored_windows=args.min_scored_windows,
            min_positive_window_rate=args.min_positive_window_rate,
        )
        candidate_config_path = _resolve_candidate_config_path(args.candidate_config)
        candidates = feature_candidates_for_keys(
            args.candidate,
            config_path=candidate_config_path,
        )
        if args.check:
            print(
                "OK: feature candidate horizon sweep check passed for "
                f"{len(candidates)} candidate(s), {len(horizons)} horizon(s), "
                f"{len(candidates) * len(horizons)} candidate/horizon cell(s)"
            )
            return 0
        if not args.database_url:
            raise FeatureStoreError("DATABASE_URL is required unless --check is used")

        if not args.skip_materialize:
            _run_command(
                _materialize_command(
                    candidates,
                    universe=args.universe,
                    candidate_config_path=candidate_config_path,
                ),
                database_url=args.database_url,
            )

        psycopg = _load_psycopg()
        with psycopg.connect(args.database_url) as connection:
            repository = HypothesisRepository(connection)
            results: list[HorizonSweepResult] = []
            for candidate in candidates:
                for horizon in horizons:
                    try:
                        results.append(
                            run_horizon_cell(
                                candidate,
                                repository=repository,
                                connection=connection,
                                database_url=args.database_url,
                                universe=args.universe,
                                horizon=horizon,
                                output_dir=args.output_dir,
                                rerun_existing=args.rerun_existing,
                                min_scored_windows=args.min_scored_windows,
                                min_positive_window_rate=(
                                    args.min_positive_window_rate
                                ),
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

    print(render_horizon_sweep_results(results, horizons=horizons))
    return 0


def run_horizon_cell(
    candidate: FeatureCandidate,
    *,
    repository: HypothesisRepository,
    connection: object,
    database_url: str,
    universe: str,
    horizon: int,
    output_dir: Path,
    rerun_existing: bool,
    min_scored_windows: int,
    min_positive_window_rate: float,
) -> HorizonSweepResult:
    hypothesis_key = horizon_hypothesis_key(candidate, horizon)
    report_path = output_dir / f"{candidate.hypothesis_key}_h{horizon}.md"
    existing = load_existing_horizon_evaluation(connection, hypothesis_key)
    if existing is not None and not rerun_existing:
        source = load_backtest_evidence_source(connection, existing.backtest_run_id)
        evidence = walk_forward_evidence_from_metrics(
            source.metrics,
            min_scored_windows=min_scored_windows,
            min_positive_window_rate=min_positive_window_rate,
        )
        return _result_from_evidence(
            candidate,
            hypothesis_key=hypothesis_key,
            horizon=horizon,
            action="skipped",
            backtest_run_id=existing.backtest_run_id,
            evidence=evidence,
            evaluation_status=existing.evaluation_status,
            failure_reason=existing.failure_reason,
            report_path=report_path,
        )

    repository.upsert_hypothesis(
        horizon_hypothesis(candidate, universe=universe, horizon=horizon)
    )
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
        hypothesis_key=hypothesis_key,
        backtest_run_id=backtest_run_id,
        evaluation_status=explicit_status,
        failure_reason=explicit_failure,
        notes=(
            "feature candidate horizon sweep v0 evaluation; "
            f"horizon={horizon}; walk_forward_status={evidence.walk_forward_status}"
        ),
    )
    return _result_from_evidence(
        candidate,
        hypothesis_key=hypothesis_key,
        horizon=horizon,
        action="evaluated",
        backtest_run_id=evaluation.backtest_run_id,
        evidence=evidence,
        evaluation_status=evaluation.evaluation_status,
        failure_reason=evaluation.failure_reason,
        report_path=report_path,
    )


def selected_horizons(raw_horizons: Sequence[int] | None) -> tuple[int, ...]:
    if not raw_horizons:
        return CANONICAL_HORIZONS
    requested = set(raw_horizons)
    invalid = sorted(requested - set(CANONICAL_HORIZONS))
    if invalid:
        allowed = ", ".join(str(horizon) for horizon in CANONICAL_HORIZONS)
        got = ", ".join(str(horizon) for horizon in invalid)
        raise FeatureStoreError(f"horizon must be one of {allowed}; got {got}")
    return tuple(horizon for horizon in CANONICAL_HORIZONS if horizon in requested)


def horizon_hypothesis_key(candidate: FeatureCandidate, horizon: int) -> str:
    if horizon == DEFAULT_BASE_HORIZON:
        return candidate.hypothesis_key
    return f"{candidate.hypothesis_key}__h{horizon}"


def horizon_hypothesis(
    candidate: FeatureCandidate,
    *,
    universe: str,
    horizon: int,
) -> HypothesisCreate:
    hypothesis_key = horizon_hypothesis_key(candidate, horizon)
    name = candidate.name if horizon == DEFAULT_BASE_HORIZON else (
        f"{candidate.name} ({horizon}d)"
    )
    return HypothesisCreate(
        hypothesis_key=hypothesis_key,
        name=name,
        thesis=candidate.thesis,
        signal_name=candidate.signal_name,
        mechanism=candidate.mechanism,
        universe_name=universe,
        horizon_days=horizon,
        target_kind="raw_return",
        status="proposed",
        metadata={
            "base_hypothesis_key": candidate.hypothesis_key,
            "candidate_pack": candidate.candidate_pack_key,
            "feature": candidate.signal_name,
            "horizon_sweep": True,
            "selection_direction": candidate.selection_direction,
        },
    )


def load_existing_horizon_evaluation(
    connection: object,
    hypothesis_key: str,
) -> ExistingHorizonEvaluation | None:
    with connection.cursor() as cursor:  # type: ignore[attr-defined]
        cursor.execute(
            """
            SELECT
                h.hypothesis_key,
                latest.backtest_run_id,
                latest.evaluation_status,
                latest.failure_reason
            FROM silver.hypotheses AS h
            LEFT JOIN LATERAL (
                SELECT
                    he.backtest_run_id,
                    he.evaluation_status,
                    he.failure_reason
                FROM silver.hypothesis_evaluations AS he
                WHERE he.hypothesis_id = h.id
                ORDER BY he.created_at DESC, he.id DESC
                LIMIT 1
            ) AS latest ON true
            WHERE h.hypothesis_key = %s
            """,
            (hypothesis_key,),
        )
        row = cursor.fetchone()
    if row is None or row[1] is None:
        return None
    return ExistingHorizonEvaluation(
        hypothesis_key=str(row[0]),
        backtest_run_id=int(row[1]),
        evaluation_status=str(row[2]),
        failure_reason=None if row[3] is None else str(row[3]),
    )


def render_horizon_sweep_results(
    results: Sequence[HorizonSweepResult],
    *,
    horizons: Sequence[int] = CANONICAL_HORIZONS,
) -> str:
    if not results:
        return "No feature candidate horizons evaluated."

    by_candidate: OrderedDict[str, list[HorizonSweepResult]] = OrderedDict()
    for result in results:
        by_candidate.setdefault(result.candidate_key, []).append(result)

    lines = [
        "Horizon Sweep Results",
        "",
        _matrix_table(by_candidate, horizons=horizons),
        "",
        "Bucket Stability:",
    ]
    for candidate_results in by_candidate.values():
        for result in sorted(candidate_results, key=lambda item: item.horizon):
            lines.append(
                "- "
                f"{result.candidate_key} @ {result.horizon}: "
                f"{result.positive_windows}/{result.scored_windows} positive, "
                f"mean_diff={_percent(result.mean_net_difference_vs_baseline)}, "
                f"verdict={_verdict(result)}, "
                f"backtest_run_id={result.backtest_run_id}"
            )
    return "\n".join(lines)


def _matrix_table(
    by_candidate: OrderedDict[str, list[HorizonSweepResult]],
    *,
    horizons: Sequence[int],
) -> str:
    headers = ("candidate", "feature", "direction", *(str(horizon) for horizon in horizons))
    rows: list[tuple[str, ...]] = []
    for candidate_key, candidate_results in by_candidate.items():
        first = candidate_results[0]
        by_horizon = {result.horizon: result for result in candidate_results}
        rows.append(
            (
                candidate_key,
                first.feature_name,
                first.selection_direction,
                *(_cell_text(by_horizon.get(horizon)) for horizon in horizons),
            )
        )
    return _table(headers, rows)


def _cell_text(result: HorizonSweepResult | None) -> str:
    if result is None:
        return "pending"
    return f"{result.action}:{result.evaluation_status}"


def _result_from_evidence(
    candidate: FeatureCandidate,
    *,
    hypothesis_key: str,
    horizon: int,
    action: str,
    backtest_run_id: int,
    evidence: object,
    evaluation_status: str,
    failure_reason: str | None,
    report_path: Path,
) -> HorizonSweepResult:
    return HorizonSweepResult(
        candidate_key=candidate.hypothesis_key,
        hypothesis_key=hypothesis_key,
        feature_name=candidate.signal_name,
        selection_direction=candidate.selection_direction,
        horizon=horizon,
        action=action,
        backtest_run_id=backtest_run_id,
        walk_forward_status=evidence.walk_forward_status,  # type: ignore[attr-defined]
        scored_windows=evidence.scored_windows,  # type: ignore[attr-defined]
        positive_windows=evidence.positive_windows,  # type: ignore[attr-defined]
        positive_window_rate=evidence.positive_window_rate,  # type: ignore[attr-defined]
        mean_net_difference_vs_baseline=(  # type: ignore[attr-defined]
            evidence.mean_net_difference_vs_baseline
        ),
        evaluation_status=evaluation_status,
        failure_reason=failure_reason,
        report_path=report_path,
    )


def _materialize_command(
    candidates: Sequence[FeatureCandidate],
    *,
    universe: str,
    candidate_config_path: Path,
) -> list[str]:
    command = [
        sys.executable,
        str(ROOT / "scripts" / "materialize_feature_candidates.py"),
        "--universe",
        universe,
        "--candidate-config",
        str(candidate_config_path),
    ]
    for candidate in candidates:
        command.extend(["--candidate", candidate.hypothesis_key])
    return command


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
        raise FeatureStoreError(f"horizon-sweep command failed: {detail}")
    return result.stdout


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
        or not 0 <= float(min_positive_window_rate) <= 1
    ):
        raise FeatureStoreError(
            "min_positive_window_rate must be between 0 and 1 inclusive"
        )


def _load_psycopg() -> object:
    try:
        import psycopg
    except ImportError as exc:
        raise FeatureStoreError(
            "psycopg is required for horizon-sweep evaluation; run `uv sync`"
        ) from exc
    return psycopg


def _resolve_candidate_config_path(path: Path) -> Path:
    candidate_path = path.expanduser()
    if candidate_path.is_absolute():
        return candidate_path
    return (Path.cwd() / candidate_path).resolve()


def _verdict(result: HorizonSweepResult) -> str:
    if result.failure_reason:
        return f"{result.evaluation_status}({result.failure_reason})"
    return result.evaluation_status


def _percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4%}"


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    header_line = "| " + " | ".join(headers) + " |"
    separator = "| " + " | ".join("---" for _ in headers) + " |"
    row_lines = ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join((header_line, separator, *row_lines))


if __name__ == "__main__":
    raise SystemExit(main())
