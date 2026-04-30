"""Deterministic markdown rendering for falsifier reports."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Mapping, Sequence

from silver.backtest.momentum_falsifier import MomentumBacktestRow
from silver.backtest.momentum_falsifier import MomentumFalsifierResult


REPORT_SCHEMA_VERSION = 3


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
class FalsifierRunIdentity:
    """Durable registry identity for the model/backtest run behind a report."""

    model_run_id: int
    model_run_key: str
    backtest_run_id: int
    backtest_run_key: str


@dataclass(frozen=True, slots=True)
class FalsifierModelWindow:
    """Durable training/test date window recorded for a model run."""

    training_start_date: date
    training_end_date: date
    test_start_date: date
    test_end_date: date
    source: str | None = None


@dataclass(frozen=True, slots=True)
class FalsifierReproducibilityMetadata:
    """Stable run metadata for reproducing a falsifier report."""

    command: str
    git_sha: str
    input_fingerprint: str
    available_at_policy_versions: Mapping[str, int]
    run_identity: FalsifierRunIdentity | None = None
    model_window: FalsifierModelWindow | None = None
    target_kind: str | None = None
    random_seed: int | None = None
    execution_assumptions: Mapping[str, Any] = field(default_factory=dict)
    report_schema_version: int = REPORT_SCHEMA_VERSION


@dataclass(frozen=True, slots=True)
class FalsifierEvidence:
    """Backtest evidence payloads mirrored from durable falsifier metadata."""

    metrics_by_regime: Mapping[str, Any] = field(default_factory=dict)
    label_scramble_metrics: Mapping[str, Any] = field(default_factory=dict)
    label_scramble_pass: bool | None = None
    multiple_comparisons_correction: str | None = None


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
    evidence: FalsifierEvidence = field(default_factory=FalsifierEvidence)


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
        "## Regime Breakdown",
        "",
        _evidence_source_table(report, "backtest_runs.metrics_by_regime"),
        "",
        _regime_breakdown_table(report.evidence.metrics_by_regime),
        "",
        "## Label-Scramble Result",
        "",
        _evidence_source_table(report, "backtest_runs.label_scramble_metrics"),
        "",
        _label_scramble_table(report.evidence),
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
            _reproducibility_rows(report),
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


def _evidence_source_table(report: FalsifierReport, metadata_field: str) -> str:
    identity = report.reproducibility.run_identity
    rows = [("Metadata field", metadata_field)]
    if identity is None:
        rows.extend(
            (
                ("model_run_id", "not supplied"),
                ("model_run_key", "not supplied"),
                ("backtest_run_id", "not supplied"),
                ("backtest_run_key", "not supplied"),
            )
        )
    else:
        rows.extend(
            (
                ("model_run_id", str(identity.model_run_id)),
                ("model_run_key", identity.model_run_key),
                ("backtest_run_id", str(identity.backtest_run_id)),
                ("backtest_run_key", identity.backtest_run_key),
            )
        )
    return _table(("Field", "Value"), tuple(rows))


def _regime_breakdown_table(metrics_by_regime: Mapping[str, Any]) -> str:
    if not metrics_by_regime:
        return "No regime evidence supplied."
    if _flat_status_mapping(metrics_by_regime):
        return _mapping_table(metrics_by_regime)

    rows: list[tuple[str, str, str, str, str, str, str]] = []
    for regime_name, payload in sorted(
        metrics_by_regime.items(),
        key=_regime_sort_key,
    ):
        if not isinstance(payload, Mapping):
            continue
        strategy_summary = _summary_mapping(payload.get("strategy_net_return"))
        baseline_summary = _summary_mapping(payload.get("baseline_net_return"))
        difference_summary = _summary_mapping(
            payload.get("net_difference_vs_baseline")
        )
        rows.append(
            (
                str(regime_name),
                _text_date_range(payload.get("start_date"), payload.get("end_date")),
                _value_text(payload.get("sample_count")),
                _percent(_optional_float(strategy_summary.get("mean"))),
                _percent(_optional_float(baseline_summary.get("mean"))),
                _percent(_optional_float(difference_summary.get("mean"))),
                _percent(_optional_float(strategy_summary.get("hit_rate"))),
            )
        )

    if not rows:
        return "No regime evidence supplied."
    return _table(
        (
            "Regime",
            "Date range",
            "Samples",
            "Strategy net mean",
            "Baseline net mean",
            "Net difference mean",
            "Strategy hit rate",
        ),
        tuple(rows),
    )


def _label_scramble_table(evidence: FalsifierEvidence) -> str:
    metrics = evidence.label_scramble_metrics
    if not metrics:
        return "No label-scramble evidence supplied."

    rows = [
        ("Status", _value_text(metrics.get("status", "unknown"))),
        (
            "Scored-row source",
            _value_text(metrics.get("scored_row_source", "not recorded")),
        ),
        (
            "Selection rule",
            _value_text(metrics.get("selection_rule", "not recorded")),
        ),
        ("Score", _value_text(metrics.get("score_name", "not recorded"))),
        ("Alternative", _value_text(metrics.get("alternative", "not recorded"))),
        ("Sample count", _value_text(metrics.get("sample_count", "not recorded"))),
        ("Group count", _value_text(metrics.get("group_count", "not recorded"))),
        ("Seed", _value_text(metrics.get("seed", "not recorded"))),
        ("Trial count", _value_text(metrics.get("trial_count", "not recorded"))),
        ("Alpha", _decimal(_optional_float(metrics.get("alpha")))),
        ("Observed score", _decimal(_optional_float(metrics.get("observed_score")))),
        ("Observed rank", _value_text(metrics.get("observed_rank", "not recorded"))),
        ("Null summary", _null_summary(metrics)),
        ("P-value", _decimal(_optional_float(metrics.get("p_value")))),
        ("Pass/fail", _pass_fail(evidence.label_scramble_pass)),
        (
            "Multiple-comparisons correction",
            evidence.multiple_comparisons_correction or "not recorded",
        ),
    ]
    reason = metrics.get("reason")
    if reason is not None:
        rows.append(("Reason", _value_text(reason)))
    return _table(("Field", "Value"), tuple(rows))


def _reproducibility_rows(report: FalsifierReport) -> tuple[tuple[str, str], ...]:
    identity = report.reproducibility.run_identity
    identity_rows: tuple[tuple[str, str], ...] = ()
    if identity is not None:
        identity_rows = (
            ("model_run_id", str(identity.model_run_id)),
            ("model_run_key", identity.model_run_key),
            ("backtest_run_id", str(identity.backtest_run_id)),
            ("backtest_run_key", identity.backtest_run_key),
        )

    model_window = report.reproducibility.model_window
    model_window_rows: tuple[tuple[str, str], ...] = ()
    if model_window is not None:
        model_window_rows = (
            (
                "Model training window",
                _date_range(
                    model_window.training_start_date,
                    model_window.training_end_date,
                ),
            ),
            (
                "Model test window",
                _date_range(
                    model_window.test_start_date,
                    model_window.test_end_date,
                ),
            ),
            ("Model window source", model_window.source or "unknown"),
        )

    return (
        ("Command", f"`{report.reproducibility.command}`"),
        *identity_rows,
        ("Git SHA", report.reproducibility.git_sha),
        ("Feature definition hash", report.feature_metadata.definition_hash),
        ("Feature set hash", report.feature_metadata.feature_set_hash),
        *model_window_rows,
        (
            "Target kind",
            report.reproducibility.target_kind or "unknown",
        ),
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
            "Execution assumptions",
            _json_mapping(report.reproducibility.execution_assumptions),
        ),
        (
            "Report schema version",
            str(report.reproducibility.report_schema_version),
        ),
    )


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


def _text_date_range(start: object, end: object) -> str:
    if start is None or end is None:
        return "n/a"
    if start == end:
        return str(start)
    return f"{start} to {end}"


def _percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.4f}%"


def _decimal(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.6f}"


def _optional_float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


def _summary_mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _flat_status_mapping(values: Mapping[str, Any]) -> bool:
    return "status" in values and not any(
        isinstance(value, Mapping) for value in values.values()
    )


def _mapping_table(values: Mapping[str, Any]) -> str:
    return _table(
        ("Field", "Value"),
        tuple((str(key), _value_text(value)) for key, value in sorted(values.items())),
    )


def _regime_sort_key(item: tuple[str, Any]) -> tuple[str, str]:
    regime_name, payload = item
    if isinstance(payload, Mapping):
        return (str(payload.get("start_date") or ""), regime_name)
    return ("", regime_name)


def _null_summary(metrics: Mapping[str, Any]) -> str:
    summary = metrics.get("null_summary")
    if isinstance(summary, str) and summary.strip():
        return summary

    scores = metrics.get("scramble_scores")
    if isinstance(scores, str | bytes) or not isinstance(scores, Sequence):
        return "not recorded"
    numeric_scores = tuple(
        float(score)
        for score in scores
        if not isinstance(score, bool) and isinstance(score, int | float)
    )
    if not numeric_scores:
        return "not recorded"

    mean = sum(numeric_scores) / len(numeric_scores)
    stddev = _sample_stddev(numeric_scores)
    return (
        f"n={len(numeric_scores)}, mean={mean:.6f}, stddev={stddev:.6f}, "
        f"min={min(numeric_scores):.6f}, max={max(numeric_scores):.6f}"
    )


def _sample_stddev(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return variance**0.5


def _pass_fail(value: bool | None) -> str:
    if value is True:
        return "pass"
    if value is False:
        return "fail"
    return "not recorded"


def _value_text(value: object) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return _decimal(value)
    if isinstance(value, Mapping):
        return _json_mapping(value)
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return "`" + json.dumps(list(value), allow_nan=False, separators=(",", ":")) + "`"
    return str(value)


def _difference(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left - right


def _json_mapping(values: Mapping[str, Any]) -> str:
    return (
        "`"
        + json.dumps(
            dict(sorted(values.items())),
            allow_nan=False,
            separators=(",", ":"),
        )
        + "`"
    )


def _float_token(value: float) -> str:
    return f"{value:.17g}"
