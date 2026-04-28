"""Deterministic markdown rendering for falsifier reports."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from typing import Mapping, Sequence

from silver.backtest.momentum_falsifier import MomentumBacktestRow
from silver.backtest.momentum_falsifier import MomentumFalsifierResult


REPORT_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class UniverseMember:
    """Point-in-time universe membership displayed in a falsifier report."""

    ticker: str
    valid_from: date
    valid_to: date | None


@dataclass(frozen=True, slots=True)
class FalsifierDataCoverage:
    """Data coverage summary for joined persisted falsifier inputs."""

    input_rows: int
    distinct_tickers: int
    distinct_asof_dates: int
    asof_start: date | None
    asof_end: date | None
    horizon_start: date | None
    horizon_end: date | None


@dataclass(frozen=True, slots=True)
class FalsifierFeatureMetadata:
    """Feature-definition metadata needed for report reproducibility."""

    name: str
    version: int
    definition_hash: str
    feature_set_hash: str


@dataclass(frozen=True, slots=True)
class FalsifierReproducibilityMetadata:
    """Stable run metadata for reproducing a falsifier report."""

    command: str
    git_sha: str
    input_fingerprint: str
    available_at_policy_versions: Mapping[str, int]
    report_schema_version: int = REPORT_SCHEMA_VERSION
    random_seed: int | None = None


@dataclass(frozen=True, slots=True)
class FalsifierReport:
    """Complete markdown-renderable Week 1 momentum report."""

    strategy: str
    horizon: int
    universe_name: str
    universe_members: tuple[UniverseMember, ...]
    data_coverage: FalsifierDataCoverage
    feature_metadata: FalsifierFeatureMetadata
    backtest_result: MomentumFalsifierResult
    reproducibility: FalsifierReproducibilityMetadata


@dataclass(frozen=True, slots=True)
class FalsifierInputCounts:
    """Counts used to explain missing persisted prerequisite data."""

    universe_members: int
    feature_values: int
    labels: int
    joined_rows: int


def render_week_1_momentum_report(report: FalsifierReport) -> str:
    """Render deterministic markdown for the Phase 1 Week 1 momentum report."""

    result = report.backtest_result
    metrics = result.headline_metrics
    lines = [
        "# Week 1 Momentum Falsifier Report",
        "",
        f"Status: {result.status}",
        "",
        "No alpha claim is made. This report records falsifier evidence only; "
        "failure and insufficient data are valid outcomes.",
        "",
        "## Run Configuration",
        "",
        _table(
            ("Field", "Value"),
            (
                ("Strategy", report.strategy),
                ("Universe", report.universe_name),
                ("Horizon", f"{report.horizon} trading sessions"),
                (
                    "Feature version",
                    f"{report.feature_metadata.name} v{report.feature_metadata.version}",
                ),
                ("Report path", "reports/falsifier/week_1_momentum.md"),
            ),
        ),
        "",
        "## Data Coverage",
        "",
        _table(
            ("Field", "Value"),
            (
                ("Joined feature/label rows", str(report.data_coverage.input_rows)),
                ("Distinct tickers", str(report.data_coverage.distinct_tickers)),
                (
                    "Distinct as-of dates",
                    str(report.data_coverage.distinct_asof_dates),
                ),
                (
                    "As-of date range",
                    _date_range(
                        report.data_coverage.asof_start,
                        report.data_coverage.asof_end,
                    ),
                ),
                (
                    "Horizon date range",
                    _date_range(
                        report.data_coverage.horizon_start,
                        report.data_coverage.horizon_end,
                    ),
                ),
            ),
        ),
        "",
        "## Universe",
        "",
        _universe_table(report.universe_members),
        "",
        "## Train/Test Windows",
        "",
        _windows_table(result),
        "",
        "## Headline Metrics",
        "",
        _table(
            ("Metric", "Value"),
            (
                ("Scored walk-forward windows", str(metrics.split_count)),
                ("Scored test dates", str(metrics.scored_test_dates)),
                ("Eligible observations", str(metrics.eligible_observations)),
                ("Selected observations", str(metrics.selected_observations)),
                (
                    "Mean strategy gross horizon return",
                    _percent(metrics.mean_strategy_gross_return),
                ),
                (
                    "Mean strategy net horizon return",
                    _percent(metrics.mean_strategy_net_return),
                ),
                (
                    "Strategy net hit rate",
                    _percent(metrics.strategy_net_hit_rate),
                ),
                (
                    "Strategy net return stddev",
                    _percent(metrics.strategy_net_return_stddev),
                ),
                (
                    "Strategy net return/stddev",
                    _decimal(metrics.strategy_net_return_to_stddev),
                ),
            ),
        ),
        "",
        "## Baseline Comparison",
        "",
        _table(
            ("Metric", "Momentum strategy", "Equal-weight universe", "Difference"),
            (
                (
                    "Mean gross horizon return",
                    _percent(metrics.mean_strategy_gross_return),
                    _percent(metrics.mean_baseline_gross_return),
                    _percent(_difference(
                        metrics.mean_strategy_gross_return,
                        metrics.mean_baseline_gross_return,
                    )),
                ),
                (
                    "Mean net horizon return",
                    _percent(metrics.mean_strategy_net_return),
                    _percent(metrics.mean_baseline_net_return),
                    _percent(metrics.mean_net_difference_vs_baseline),
                ),
            ),
        ),
        "",
        "## Costs Assumption",
        "",
        _table(
            ("Field", "Value"),
            (
                (
                    "Round-trip cost",
                    f"{result.round_trip_cost_bps:.2f} bps per rebalance",
                ),
                (
                    "Application",
                    "Subtracted from strategy and equal-weight baseline returns "
                    "for each scored test date.",
                ),
            ),
        ),
        "",
        "## Failure Modes",
        "",
        _failure_mode_lines(result.failure_modes),
        "",
        "## Reproducibility",
        "",
        _table(
            ("Field", "Value"),
            (
                ("Command", f"`{report.reproducibility.command}`"),
                ("Git SHA", report.reproducibility.git_sha),
                ("Feature definition hash", report.feature_metadata.definition_hash),
                ("Feature set hash", report.feature_metadata.feature_set_hash),
                ("Input fingerprint", report.reproducibility.input_fingerprint),
                (
                    "Available-at policy versions",
                    _json_mapping(report.reproducibility.available_at_policy_versions),
                ),
                (
                    "Random seed",
                    (
                        "none"
                        if report.reproducibility.random_seed is None
                        else str(report.reproducibility.random_seed)
                    ),
                ),
                (
                    "Report schema version",
                    str(report.reproducibility.report_schema_version),
                ),
            ),
        ),
    ]
    return "\n".join(lines) + "\n"


def coverage_from_rows(rows: Sequence[MomentumBacktestRow]) -> FalsifierDataCoverage:
    """Build deterministic coverage metadata from joined feature/label rows."""

    normalized_rows = tuple(rows)
    if not normalized_rows:
        return FalsifierDataCoverage(
            input_rows=0,
            distinct_tickers=0,
            distinct_asof_dates=0,
            asof_start=None,
            asof_end=None,
            horizon_start=None,
            horizon_end=None,
        )

    asof_dates = tuple(sorted({row.asof_date for row in normalized_rows}))
    horizon_dates = tuple(sorted({row.horizon_date for row in normalized_rows}))
    return FalsifierDataCoverage(
        input_rows=len(normalized_rows),
        distinct_tickers=len({row.ticker for row in normalized_rows}),
        distinct_asof_dates=len(asof_dates),
        asof_start=asof_dates[0],
        asof_end=asof_dates[-1],
        horizon_start=horizon_dates[0],
        horizon_end=horizon_dates[-1],
    )


def fingerprint_momentum_inputs(rows: Sequence[MomentumBacktestRow]) -> str:
    """Return a stable SHA-256 fingerprint for joined falsifier inputs."""

    payload = [
        {
            "ticker": row.ticker,
            "asof_date": row.asof_date.isoformat(),
            "horizon_date": row.horizon_date.isoformat(),
            "feature_value": _float_token(row.feature_value),
            "realized_return": _float_token(row.realized_return),
        }
        for row in sorted(rows, key=lambda item: (item.asof_date, item.ticker))
    ]
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def missing_prerequisite_message(
    counts: FalsifierInputCounts,
    *,
    strategy: str,
    horizon: int,
    universe: str,
) -> str | None:
    """Explain which materialization prerequisite is missing, if any."""

    if counts.universe_members == 0:
        return (
            f"Missing prerequisite data: universe `{universe}` has no persisted "
            "membership rows. Run `python scripts/seed_reference_data.py` after "
            "applying migrations."
        )
    if counts.feature_values == 0:
        return (
            f"Missing prerequisite data: no persisted `{strategy}` feature values "
            f"exist for universe `{universe}`. Run the momentum feature "
            "materialization step after daily prices are normalized."
        )
    if counts.labels == 0:
        return (
            "Missing prerequisite data: no forward-return labels exist for "
            f"horizon {horizon} in universe `{universe}`. Run the forward-label "
            "materialization step after daily prices are normalized."
        )
    if counts.joined_rows == 0:
        return (
            "Missing prerequisite data: persisted feature values and "
            "forward-return labels do not overlap by security/as-of date. "
            "Re-run the feature and label materialization steps over the same "
            "calendar coverage."
        )
    return None


def _windows_table(result: MomentumFalsifierResult) -> str:
    if not result.windows:
        return "No scorable train/test windows."

    return _table(
        (
            "Split",
            "Train window",
            "Test window",
            "Train rows",
            "Test rows",
            "Scored dates",
            "Selected obs",
            "Strategy net",
            "Baseline net",
        ),
        tuple(
            (
                str(window.split_index),
                _date_range(window.train_start, window.train_end),
                _date_range(window.test_start, window.test_end),
                str(window.train_observations),
                str(window.test_observations),
                str(window.scored_dates),
                str(window.selected_observations),
                _percent(window.strategy_net_return),
                _percent(window.baseline_net_return),
            )
            for window in result.windows
        ),
    )


def _universe_table(members: Sequence[UniverseMember]) -> str:
    if not members:
        return "No universe members."

    return _table(
        ("Ticker", "Valid from", "Valid to"),
        tuple(
            (
                member.ticker,
                member.valid_from.isoformat(),
                member.valid_to.isoformat() if member.valid_to is not None else "open",
            )
            for member in sorted(members, key=lambda item: item.ticker)
        ),
    )


def _failure_mode_lines(failure_modes: Sequence[str]) -> str:
    if not failure_modes:
        return "- None triggered by the thin Phase 1 checks."
    return "\n".join(f"- {failure_mode}" for failure_mode in failure_modes)


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    header_line = "| " + " | ".join(headers) + " |"
    separator = "| " + " | ".join("---" for _ in headers) + " |"
    row_lines = ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join((header_line, separator, *row_lines))


def _date_range(start: date | None, end: date | None) -> str:
    if start is None or end is None:
        return "n/a"
    if start == end:
        return start.isoformat()
    return f"{start.isoformat()} to {end.isoformat()}"


def _percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.4f}%"


def _decimal(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.6f}"


def _difference(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left - right


def _json_mapping(values: Mapping[str, int]) -> str:
    return "`" + json.dumps(dict(sorted(values.items())), separators=(",", ":")) + "`"


def _float_token(value: float) -> str:
    return f"{value:.17g}"
