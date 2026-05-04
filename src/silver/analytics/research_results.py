"""Read-only research results report over persisted hypothesis evidence."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from silver.analytics.hypothesis_evaluation_explainer import (
    HypothesisEvaluationExplanationError,
    TickerAttribution,
    load_hypothesis_evaluation_explanation,
)
from silver.features.candidate_pack import (
    DEFAULT_CANDIDATE_CONFIG_PATH,
    FUNDAMENTAL_MATERIALIZERS,
    FeatureCandidate,
    load_feature_candidates,
)
from silver.time.trading_calendar import CANONICAL_HORIZONS


PROMISING_DEEP_DIVE_BASE_HYPOTHESIS_KEY = "avg_dollar_volume_63"
PROMISING_DEEP_DIVE_HORIZON_DAYS = 252
PROMISING_DEEP_DIVE_MOMENTUM_BASE_KEYS = (
    "momentum_12_1",
    "momentum_6_1",
    "momentum_3_0",
)
PROMISING_DEEP_DIVE_COMPARISON_HORIZONS = (63, 126, 252)


class ResearchResultsError(ValueError):
    """Raised when persisted research results cannot be summarized."""


class ResearchResultsJsonClient(Protocol):
    """Minimal read-only JSON query client used by the report loader."""

    def fetch_json(self, sql: str) -> Any:
        """Return one decoded JSON value for a SQL query."""
        ...


@dataclass(frozen=True, slots=True)
class WalkForwardBucket:
    """One persisted walk-forward test-window result."""

    test_start: str | None
    test_end: str | None
    net_difference_vs_baseline: float

    @property
    def sign(self) -> str:
        return "+" if self.net_difference_vs_baseline > 0 else "-"

    @property
    def year(self) -> str:
        if self.test_start and len(self.test_start) >= 4:
            return self.test_start[:4]
        return "unknown"


@dataclass(frozen=True, slots=True)
class ResearchResultRow:
    """One hypothesis plus its latest linked falsifier evidence."""

    hypothesis_key: str
    base_hypothesis_key: str
    hypothesis_name: str
    family: str
    feature_name: str
    horizon_days: int | None
    target_kind: str | None
    selection_direction: str
    verdict: str
    failure_reason: str
    strategy_net_return: float | None
    baseline_net_return: float | None
    net_difference_vs_baseline: float | None
    label_scramble_pass: bool | None
    label_scramble_p_value: float | None
    label_scramble_alpha: float | None
    round_trip_cost_bps: float | None
    backtest_run_id: int | None
    backtest_run_key: str | None
    model_run_key: str | None
    scored_test_dates: int | None
    walk_forward_buckets: tuple[WalkForwardBucket, ...]

    @property
    def tested(self) -> bool:
        return self.backtest_run_id is not None


@dataclass(frozen=True, slots=True)
class SelectionAttributionSet:
    """Selected-ticker attribution for one reconstructed backtest path."""

    hypothesis_key: str
    base_hypothesis_key: str
    horizon_days: int
    tickers: tuple[TickerAttribution, ...]


@dataclass(frozen=True, slots=True)
class ResearchResultsReport:
    """Complete operator-facing research results rollup."""

    results: tuple[ResearchResultRow, ...]
    candidates: tuple[FeatureCandidate, ...] = ()
    selection_attributions: tuple[SelectionAttributionSet, ...] = ()

    @property
    def tested_count(self) -> int:
        return sum(1 for result in self.results if result.tested)

    @property
    def untested_count(self) -> int:
        return len(self.results) - self.tested_count

    @property
    def families(self) -> tuple[str, ...]:
        return tuple(sorted({result.family for result in self.results}, key=_family_key))

    @property
    def horizons_tested(self) -> tuple[int, ...]:
        return tuple(
            sorted(
                {
                    result.horizon_days
                    for result in self.results
                    if result.tested and result.horizon_days is not None
                }
            )
        )

    @property
    def verdict_counts(self) -> Mapping[str, int]:
        return Counter(result.verdict for result in self.results)


def load_research_results_report(
    client: ResearchResultsJsonClient,
    *,
    candidate_config_path: Path | str = DEFAULT_CANDIDATE_CONFIG_PATH,
    candidates: Sequence[FeatureCandidate] | None = None,
) -> ResearchResultsReport:
    """Load hypothesis results and enrich them with candidate-pack metadata."""

    candidate_rows = (
        tuple(candidates)
        if candidates is not None
        else load_feature_candidates(candidate_config_path)
    )
    candidates_by_key = {candidate.hypothesis_key: candidate for candidate in candidate_rows}
    payload = client.fetch_json(_research_results_sql())
    if not isinstance(payload, list):
        raise ResearchResultsError("research results query returned non-list JSON")
    results = tuple(
        _research_result_row(raw, candidates_by_key)
        for raw in payload
    )
    return ResearchResultsReport(
        candidates=candidate_rows,
        results=results,
        selection_attributions=_load_deep_dive_selection_attributions(
            client,
            results,
        ),
    )


def render_research_results_report(report: ResearchResultsReport) -> str:
    """Render an operator-facing markdown report."""

    if not isinstance(report, ResearchResultsReport):
        raise ResearchResultsError("report must be a ResearchResultsReport")

    lines = [
        "Research Results v0",
        "",
        "Summary:",
        f"- Hypotheses: {len(report.results)}",
        f"- Tested hypotheses: {report.tested_count}",
        f"- Untested hypotheses: {report.untested_count}",
        f"- Families: {_list_text(report.families)}",
        f"- Horizons tested: {_horizon_text(report.horizons_tested)}",
        "",
        "By Family:",
        _family_table(report.results),
        "",
        "By Verdict:",
        _verdict_table(report.results),
        "",
        "Horizon Matrix:",
        _horizon_matrix(report),
        "",
        "Bucket Heatmaps:",
        _bucket_heatmap_table(report.results),
        "",
        "Promising Candidate Summary:",
        *_promising_candidate_summary_lines(report.results),
        "",
        "Promising Candidate Review:",
        _promising_candidate_review_table(report.results),
        "",
        "Promising Deep Dive v0:",
        *_promising_deep_dive_lines(
            report.results,
            report.selection_attributions,
        ),
        "",
        "Results:",
        _results_table(report.results),
        "",
        "Rejection Reasons:",
        _reason_table(report.results),
        "",
        "Untested Hypotheses:",
        _untested_table(report.results),
        "",
        "Suggested Next Tests:",
        *_suggestion_lines(report.results),
        "",
        "Provenance Notes:",
        "- Source of truth is the latest linked hypothesis evaluation and its durable backtest/model rows.",
        "- Verdicts are conservative and derived from stored status, baseline comparison, and label-scramble evidence.",
        "- Generated report files are operator summaries; durable registry rows remain authoritative.",
        "",
        "Reason Glossary:",
        _reason_glossary(report.results),
    ]
    return "\n".join(lines) + "\n"


def _research_result_row(
    raw: object,
    candidates_by_key: Mapping[str, FeatureCandidate],
) -> ResearchResultRow:
    row = _required_mapping(raw, "research result row")
    hypothesis_key = _required_str(row, "hypothesis_key")
    hypothesis_metadata = _mapping(row.get("hypothesis_metadata"), "hypothesis_metadata")
    base_hypothesis_key = _base_hypothesis_key(
        hypothesis_key,
        hypothesis_metadata=hypothesis_metadata,
        candidates_by_key=candidates_by_key,
    )
    candidate = candidates_by_key.get(base_hypothesis_key)
    backtest_parameters = _mapping(row.get("backtest_parameters"), "backtest_parameters")
    model_parameters = _mapping(row.get("model_parameters"), "model_parameters")
    metrics = _mapping(row.get("backtest_metrics"), "backtest_metrics")
    baseline_metrics = _mapping(row.get("baseline_metrics"), "baseline_metrics")
    label_scramble_metrics = _mapping(
        row.get("label_scramble_metrics"),
        "label_scramble_metrics",
    )
    backtest_cost_assumptions = _mapping(
        row.get("backtest_cost_assumptions"),
        "backtest_cost_assumptions",
    )

    strategy = _parameter_str(backtest_parameters, "strategy") or _parameter_str(
        model_parameters,
        "strategy",
    )
    feature_name = (
        strategy
        or (candidate.signal_name if candidate is not None else None)
        or _optional_str(row.get("hypothesis_signal_name"), "hypothesis_signal_name")
        or "unknown"
    )
    selection_direction = (
        _selection_direction(
            _parameter_str(backtest_parameters, "selection_direction")
            or _parameter_str(model_parameters, "selection_direction")
            or _optional_str(
                hypothesis_metadata.get("selection_direction"),
                "hypothesis_metadata.selection_direction",
            )
            or (candidate.selection_direction if candidate is not None else "high")
        )
    )
    strategy_net = _optional_float(
        metrics.get("mean_strategy_net_horizon_return"),
        "mean_strategy_net_horizon_return",
    )
    baseline_net = _baseline_net_return(baseline_metrics)
    difference = _baseline_difference(
        baseline_metrics,
        strategy_net_return=strategy_net,
        baseline_net_return=baseline_net,
    )
    backtest_status = _optional_str(row.get("backtest_status"), "backtest_status")
    evaluation_status = _optional_str(row.get("evaluation_status"), "evaluation_status")
    explicit_failure = _optional_str(row.get("failure_reason"), "failure_reason")
    label_scramble_pass = _optional_bool(
        row.get("label_scramble_pass"),
        "label_scramble_pass",
    )
    backtest_run_id = _optional_int(row.get("backtest_run_id"), "backtest_run_id")
    verdict, failure_reason = _derive_verdict(
        backtest_run_id=backtest_run_id,
        backtest_status=backtest_status,
        evaluation_status=evaluation_status,
        explicit_failure=explicit_failure,
        label_scramble_pass=label_scramble_pass,
        net_difference_vs_baseline=difference,
    )

    return ResearchResultRow(
        hypothesis_key=hypothesis_key,
        base_hypothesis_key=base_hypothesis_key,
        hypothesis_name=_required_str(row, "hypothesis_name"),
        family=_family_for(candidate, feature_name),
        feature_name=feature_name,
        horizon_days=(
            _optional_int(row.get("backtest_horizon_days"), "backtest_horizon_days")
            or _optional_int(row.get("hypothesis_horizon_days"), "hypothesis_horizon_days")
        ),
        target_kind=(
            _optional_str(row.get("backtest_target_kind"), "backtest_target_kind")
            or _optional_str(row.get("hypothesis_target_kind"), "hypothesis_target_kind")
        ),
        selection_direction=selection_direction,
        verdict=verdict,
        failure_reason=failure_reason,
        strategy_net_return=strategy_net,
        baseline_net_return=baseline_net,
        net_difference_vs_baseline=difference,
        label_scramble_pass=label_scramble_pass,
        label_scramble_p_value=_optional_float(
            label_scramble_metrics.get("p_value"),
            "label_scramble_metrics.p_value",
        ),
        label_scramble_alpha=_optional_float(
            label_scramble_metrics.get("alpha"),
            "label_scramble_metrics.alpha",
        ),
        round_trip_cost_bps=_optional_float(
            backtest_cost_assumptions.get("round_trip_cost_bps"),
            "backtest_cost_assumptions.round_trip_cost_bps",
        ),
        backtest_run_id=backtest_run_id,
        backtest_run_key=_optional_str(row.get("backtest_run_key"), "backtest_run_key"),
        model_run_key=_optional_str(row.get("model_run_key"), "model_run_key"),
        scored_test_dates=_optional_int(
            metrics.get("scored_test_dates"),
            "scored_test_dates",
        ),
        walk_forward_buckets=_walk_forward_buckets(metrics),
    )


def _load_deep_dive_selection_attributions(
    client: ResearchResultsJsonClient,
    results: Sequence[ResearchResultRow],
) -> tuple[SelectionAttributionSet, ...]:
    results_by_cell = _results_by_cell(results)
    attributions: list[SelectionAttributionSet] = []
    for base_key, horizon in _deep_dive_selection_cells():
        result = results_by_cell.get((base_key, horizon))
        if result is None or result.backtest_run_id is None:
            continue
        try:
            explanation = load_hypothesis_evaluation_explanation(
                client,
                backtest_run_id=result.backtest_run_id,
            )
        except HypothesisEvaluationExplanationError:
            continue
        attributions.append(
            SelectionAttributionSet(
                hypothesis_key=result.hypothesis_key,
                base_hypothesis_key=result.base_hypothesis_key,
                horizon_days=horizon,
                tickers=explanation.ticker_attribution,
            )
        )
    return tuple(attributions)


def _deep_dive_selection_cells() -> tuple[tuple[str, int], ...]:
    cells = [
        (
            PROMISING_DEEP_DIVE_BASE_HYPOTHESIS_KEY,
            PROMISING_DEEP_DIVE_HORIZON_DAYS,
        )
    ]
    cells.extend(
        (base_key, PROMISING_DEEP_DIVE_HORIZON_DAYS)
        for base_key in PROMISING_DEEP_DIVE_MOMENTUM_BASE_KEYS
    )
    return tuple(dict.fromkeys(cells))


def _derive_verdict(
    *,
    backtest_run_id: int | None,
    backtest_status: str | None,
    evaluation_status: str | None,
    explicit_failure: str | None,
    label_scramble_pass: bool | None,
    net_difference_vs_baseline: float | None,
) -> tuple[str, str]:
    if backtest_run_id is None:
        return "untested", "not_run"
    if backtest_status == "running":
        return "running", explicit_failure or "running"
    if backtest_status == "failed":
        return "failed", explicit_failure or "backtest_failed"
    if backtest_status == "insufficient_data":
        return "insufficient_data", explicit_failure or "insufficient_data"
    if backtest_status != "succeeded":
        return "failed", explicit_failure or f"unsupported_status:{backtest_status}"

    if explicit_failure and evaluation_status == "rejected":
        return "rejected", explicit_failure
    if label_scramble_pass is False:
        return "rejected", explicit_failure or "label_scramble_failed"
    if net_difference_vs_baseline is None:
        return "rejected", explicit_failure or "baseline_missing"
    if net_difference_vs_baseline <= 0:
        return "rejected", explicit_failure or "baseline_failed"
    if evaluation_status == "accepted":
        return "accepted", "passed"
    return "promising", "passed"


def _research_results_sql() -> str:
    return """
WITH latest_evaluations AS (
    SELECT DISTINCT ON (he.hypothesis_id)
        he.*
    FROM silver.hypothesis_evaluations he
    ORDER BY he.hypothesis_id, he.created_at DESC, he.id DESC
)
SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row.hypothesis_key), '[]'::jsonb)::text
FROM (
    SELECT
        h.hypothesis_key,
        h.name AS hypothesis_name,
        h.status AS hypothesis_status,
        h.signal_name AS hypothesis_signal_name,
        h.universe_name AS hypothesis_universe_name,
        h.horizon_days AS hypothesis_horizon_days,
        h.target_kind AS hypothesis_target_kind,
        h.metadata AS hypothesis_metadata,
        le.evaluation_status,
        le.failure_reason,
        le.notes AS evaluation_notes,
        br.id AS backtest_run_id,
        br.backtest_run_key,
        br.status AS backtest_status,
        br.universe_name AS backtest_universe_name,
        br.horizon_days AS backtest_horizon_days,
        br.target_kind AS backtest_target_kind,
        br.parameters AS backtest_parameters,
        br.metrics AS backtest_metrics,
        br.baseline_metrics,
        br.cost_assumptions AS backtest_cost_assumptions,
        br.label_scramble_metrics,
        br.label_scramble_pass,
        br.multiple_comparisons_correction,
        mr.id AS model_run_id,
        mr.model_run_key,
        mr.status AS model_status,
        mr.parameters AS model_parameters,
        mr.feature_set_hash AS model_feature_set_hash,
        mr.available_at_policy_versions AS model_available_at_policy_versions,
        mr.input_fingerprints AS model_input_fingerprints
    FROM silver.hypotheses h
    LEFT JOIN latest_evaluations le ON le.hypothesis_id = h.id
    LEFT JOIN silver.backtest_runs br ON br.id = le.backtest_run_id
    LEFT JOIN silver.model_runs mr ON mr.id = COALESCE(br.model_run_id, le.model_run_id)
) row;
""".strip()


def _family_for(candidate: FeatureCandidate | None, feature_name: str) -> str:
    if candidate is not None:
        if candidate.materializer in FUNDAMENTAL_MATERIALIZERS:
            return "fundamentals"
        return "price"
    if feature_name in {
        "revenue_growth_yoy",
        "gross_margin",
        "operating_margin",
        "net_margin",
        "diluted_shares_change_yoy",
    }:
        return "fundamentals"
    if feature_name == "unknown":
        return "unknown"
    return "price"


def _base_hypothesis_key(
    hypothesis_key: str,
    *,
    hypothesis_metadata: Mapping[str, Any],
    candidates_by_key: Mapping[str, FeatureCandidate],
) -> str:
    metadata_base = _optional_str(
        hypothesis_metadata.get("base_hypothesis_key"),
        "hypothesis_metadata.base_hypothesis_key",
    )
    if metadata_base:
        return metadata_base
    if hypothesis_key in candidates_by_key:
        return hypothesis_key
    suffix_marker = "__h"
    if suffix_marker in hypothesis_key:
        candidate_key = hypothesis_key.rsplit(suffix_marker, maxsplit=1)[0]
        if candidate_key in candidates_by_key:
            return candidate_key
    return hypothesis_key


def _baseline_net_return(baseline_metrics: Mapping[str, Any]) -> float | None:
    equal_weight = baseline_metrics.get("equal_weight_universe")
    if not isinstance(equal_weight, Mapping):
        return None
    return _optional_float(
        equal_weight.get("mean_net_horizon_return"),
        "equal_weight_universe.mean_net_horizon_return",
    )


def _baseline_difference(
    baseline_metrics: Mapping[str, Any],
    *,
    strategy_net_return: float | None,
    baseline_net_return: float | None,
) -> float | None:
    comparison = baseline_metrics.get("strategy_vs_equal_weight_universe")
    if isinstance(comparison, Mapping):
        value = _optional_float(
            comparison.get("mean_net_difference"),
            "strategy_vs_equal_weight_universe.mean_net_difference",
        )
        if value is not None:
            return value
    if strategy_net_return is None or baseline_net_return is None:
        return None
    return strategy_net_return - baseline_net_return


def _walk_forward_buckets(metrics: Mapping[str, Any]) -> tuple[WalkForwardBucket, ...]:
    raw_windows = metrics.get("walk_forward_windows")
    if raw_windows is None:
        return ()
    if not isinstance(raw_windows, list):
        raise ResearchResultsError("walk_forward_windows must be a list")
    buckets: list[WalkForwardBucket] = []
    for index, raw_window in enumerate(raw_windows):
        if not isinstance(raw_window, Mapping):
            raise ResearchResultsError("walk_forward_windows entries must be objects")
        net_difference = _window_net_difference(raw_window, index=index)
        buckets.append(
            WalkForwardBucket(
                test_start=_optional_str(
                    raw_window.get("test_start"),
                    f"walk_forward_windows[{index}].test_start",
                ),
                test_end=_optional_str(
                    raw_window.get("test_end"),
                    f"walk_forward_windows[{index}].test_end",
                ),
                net_difference_vs_baseline=net_difference,
            )
        )
    return tuple(buckets)


def _window_net_difference(window: Mapping[str, Any], *, index: int) -> float:
    value = _optional_float(
        window.get("net_difference_vs_baseline"),
        f"walk_forward_windows[{index}].net_difference_vs_baseline",
    )
    if value is not None:
        return value
    strategy = _optional_float(
        window.get("strategy_net_return"),
        f"walk_forward_windows[{index}].strategy_net_return",
    )
    baseline = _optional_float(
        window.get("baseline_net_return"),
        f"walk_forward_windows[{index}].baseline_net_return",
    )
    if strategy is None or baseline is None:
        raise ResearchResultsError(
            "walk_forward_windows entries must include net_difference_vs_baseline "
            "or strategy/baseline net returns"
        )
    return strategy - baseline


def _family_table(results: Sequence[ResearchResultRow]) -> str:
    rows: list[tuple[str, ...]] = []
    for family in sorted({result.family for result in results}, key=_family_key):
        family_results = [result for result in results if result.family == family]
        rows.append(
            (
                family,
                str(len(family_results)),
                str(_count_verdict(family_results, "accepted")),
                str(_count_verdict(family_results, "promising")),
                str(_count_verdict(family_results, "rejected")),
                str(_count_verdict(family_results, "insufficient_data")),
                str(_count_verdict(family_results, "untested")),
            )
        )
    if not rows:
        return "No hypotheses found."
    return _table(
        (
            "Family",
            "Total",
            "Accepted",
            "Promising",
            "Rejected",
            "Insufficient",
            "Untested",
        ),
        rows,
    )


def _verdict_table(results: Sequence[ResearchResultRow]) -> str:
    if not results:
        return "No verdicts found."
    counts = Counter(result.verdict for result in results)
    rows = [
        (verdict, str(counts[verdict]))
        for verdict in sorted(counts, key=_verdict_key)
    ]
    return _table(("Verdict", "Count"), rows)


def _results_table(results: Sequence[ResearchResultRow]) -> str:
    if not results:
        return "No hypotheses found."
    rows = [
        (
            result.hypothesis_key,
            result.family,
            result.feature_name,
            _int_or_na(result.horizon_days),
            result.selection_direction,
            result.verdict,
            _percent(result.strategy_net_return),
            _percent(result.baseline_net_return),
            _percent(result.net_difference_vs_baseline),
            result.failure_reason,
            _int_or_na(result.backtest_run_id),
        )
        for result in results
    ]
    return _table(
        (
            "Hypothesis",
            "Family",
            "Feature",
            "Horizon",
            "Direction",
            "Verdict",
            "Strategy",
            "Baseline",
            "Difference",
            "Failure reason",
            "Backtest",
        ),
        rows,
    )


def _horizon_matrix(report: ResearchResultsReport) -> str:
    results_by_cell: dict[tuple[str, int], ResearchResultRow] = {}
    for result in report.results:
        if result.horizon_days is None:
            continue
        results_by_cell[(result.base_hypothesis_key, result.horizon_days)] = result

    rows: list[tuple[str, ...]] = []
    seen: set[str] = set()
    for candidate in report.candidates:
        seen.add(candidate.hypothesis_key)
        rows.append(
            _horizon_matrix_row(
                base_key=candidate.hypothesis_key,
                family=_family_for(candidate, candidate.signal_name),
                feature_name=candidate.signal_name,
                results_by_cell=results_by_cell,
            )
        )

    unknown_base_keys = sorted(
        {
            result.base_hypothesis_key
            for result in report.results
            if result.base_hypothesis_key not in seen
        }
    )
    for base_key in unknown_base_keys:
        base_results = [
            result for result in report.results if result.base_hypothesis_key == base_key
        ]
        first = base_results[0]
        rows.append(
            _horizon_matrix_row(
                base_key=base_key,
                family=first.family,
                feature_name=first.feature_name,
                results_by_cell=results_by_cell,
            )
        )

    if not rows:
        return "No candidate horizon cells found."
    return _table(
        ("Hypothesis", "Family", "Feature", "5", "21", "63", "126", "252"),
        rows,
    )


def _horizon_matrix_row(
    *,
    base_key: str,
    family: str,
    feature_name: str,
    results_by_cell: Mapping[tuple[str, int], ResearchResultRow],
) -> tuple[str, ...]:
    return (
        base_key,
        family,
        feature_name,
        *(
            _horizon_cell(results_by_cell.get((base_key, horizon)))
            for horizon in CANONICAL_HORIZONS
        ),
    )


def _horizon_cell(result: ResearchResultRow | None) -> str:
    if result is None:
        return "pending"
    if result.verdict in {"accepted", "promising", "running", "untested"}:
        return result.verdict
    return f"{result.verdict}:{result.failure_reason}"


def _bucket_heatmap_table(results: Sequence[ResearchResultRow]) -> str:
    rows: list[tuple[str, ...]] = []
    for result in sorted(
        (row for row in results if row.walk_forward_buckets),
        key=lambda row: (
            row.base_hypothesis_key,
            -1 if row.horizon_days is None else row.horizon_days,
        ),
    ):
        positive = sum(1 for bucket in result.walk_forward_buckets if bucket.sign == "+")
        rows.append(
            (
                result.base_hypothesis_key,
                _int_or_na(result.horizon_days),
                f"{positive}/{len(result.walk_forward_buckets)}",
                _bucket_heatmap(result.walk_forward_buckets),
                "`+` beat baseline; `-` failed baseline.",
            )
        )
    if not rows:
        return "No walk-forward bucket windows found."
    return _table(
        ("Hypothesis", "Horizon", "Positive buckets", "Heatmap", "Legend"),
        rows,
    )


def _bucket_heatmap(buckets: Sequence[WalkForwardBucket]) -> str:
    by_year: dict[str, list[str]] = {}
    for bucket in buckets:
        by_year.setdefault(bucket.year, []).append(bucket.sign)
    return " ".join(
        f"{year}:{''.join(signs)}" for year, signs in sorted(by_year.items())
    )


def _promising_candidate_review_table(results: Sequence[ResearchResultRow]) -> str:
    promising = _promising_results(results)
    if not promising:
        return "No promising cells found."

    results_by_cell = _results_by_cell(results)
    rows: list[tuple[str, ...]] = []
    for result in promising:
        recommendation, reason = _promising_recommendation(result, results_by_cell)
        rows.append(
            (
                result.hypothesis_key,
                _int_or_na(result.horizon_days),
                _positive_bucket_text(result),
                _percent(result.net_difference_vs_baseline),
                _adjacent_horizon_text(result, results_by_cell),
                _label_scramble_text(result),
                _cost_sensitivity_text(result),
                recommendation,
                reason,
            )
        )
    return _table(
        (
            "Cell",
            "Horizon",
            "Positive buckets",
            "Edge",
            "Adjacent horizons",
            "Label scramble",
            "Cost sensitivity",
            "Recommendation",
            "Reason",
        ),
        rows,
    )


def _promising_candidate_summary_lines(
    results: Sequence[ResearchResultRow],
) -> list[str]:
    promising = _promising_results(results)
    if not promising:
        return ["No promising cells found."]

    results_by_cell = _results_by_cell(results)
    lines: list[str] = []
    for index, result in enumerate(promising, start=1):
        recommendation, reason = _promising_recommendation(result, results_by_cell)
        if lines:
            lines.append("")
        lines.extend(
            [
                f"{index}. {result.hypothesis_key}",
                f"   recommendation: {recommendation}",
                f"   horizon: {_int_or_na(result.horizon_days)} trading sessions",
                f"   edge: {_signed_percent(result.net_difference_vs_baseline)}",
                f"   buckets: {_positive_bucket_text(result)}",
                f"   label scramble: {_label_scramble_text(result)}",
                f"   cost sensitivity: {_cost_sensitivity_text(result)}",
                f"   reason: {reason}",
            ]
        )
    return lines


def _promising_deep_dive_lines(
    results: Sequence[ResearchResultRow],
    selection_attributions: Sequence[SelectionAttributionSet],
) -> list[str]:
    results_by_cell = _results_by_cell(results)
    target_attribution = _selection_attribution_for(
        selection_attributions,
        base_key=PROMISING_DEEP_DIVE_BASE_HYPOTHESIS_KEY,
        horizon=PROMISING_DEEP_DIVE_HORIZON_DAYS,
    )
    target = results_by_cell.get(
        (
            PROMISING_DEEP_DIVE_BASE_HYPOTHESIS_KEY,
            PROMISING_DEEP_DIVE_HORIZON_DAYS,
        )
    )
    target_key = (
        f"{PROMISING_DEEP_DIVE_BASE_HYPOTHESIS_KEY}"
        f"__h{PROMISING_DEEP_DIVE_HORIZON_DAYS}"
    )
    if target is None:
        return [
            f"No stored row found for `{target_key}`. Run the horizon sweep "
            "before opening the first deep dive."
        ]

    recommendation, reason = _deep_dive_recommendation(
        target,
        results_by_cell,
        target_attribution,
    )
    lines = [
        f"Cell: {target.hypothesis_key}",
        f"Recommendation: {recommendation}",
        f"Reason: {reason}",
        "",
        "Summary:",
        (
            "- edge: "
            f"{_signed_percent(target.net_difference_vs_baseline)} versus "
            "equal-weight baseline"
        ),
        f"- bucket breadth: {_positive_bucket_text(target)}",
        f"- temporal concentration: {_temporal_concentration_text(target)}",
        (
            "- ticker concentration: "
            f"{_ticker_concentration_text(target_attribution)}"
        ),
        f"- adjacent horizon read: {_deep_dive_adjacent_summary(target, results_by_cell)}",
        f"- cost sensitivity: {_cost_sensitivity_text(target)}",
        f"- overlap risk: {_momentum_selection_overlap_summary(target_attribution, selection_attributions)}",
        "",
        "Year/Bucket Drivers:",
        _deep_dive_year_driver_table(target),
        "",
        "Weakest Buckets:",
        _weakest_bucket_table(target),
        "",
        "Adjacent Horizon Comparison:",
        _deep_dive_adjacent_table(target, results_by_cell),
        "",
        "Momentum Overlap Proxy:",
        (
            "- This compares stored verdict, edge, and bucket patterns only; "
            "it is not ticker-overlap evidence."
        ),
        *_momentum_overlap_proxy_lines(results_by_cell),
        "",
        "Selected Tickers:",
        *_selected_ticker_lines(target_attribution, selection_attributions),
        "",
        "Exposure Notes:",
        *_exposure_note_lines(target_attribution, selection_attributions),
        (
            "- h252 passing while h126 is not validated means the annual "
            "horizon needs driver inspection before any promotion."
        ),
        "",
        "Decision:",
        f"- {recommendation}: {reason}",
    ]
    return lines


def _deep_dive_recommendation(
    target: ResearchResultRow,
    results_by_cell: Mapping[tuple[str, int], ResearchResultRow],
    target_attribution: SelectionAttributionSet | None,
) -> tuple[str, str]:
    if target.verdict != "promising":
        return "demote", f"target verdict is {_review_verdict(target)}"

    edge = target.net_difference_vs_baseline
    if edge is None:
        return "demote", "edge versus baseline is missing"
    if edge <= 0:
        return "demote", "edge versus baseline is not positive"

    cost_multiple = _cost_edge_multiple(target)
    if cost_multiple is not None and cost_multiple < 1:
        return "demote", "edge is smaller than one current round-trip cost"

    positive_rate = _positive_bucket_rate(target)
    if positive_rate is not None and positive_rate < 0.60:
        return "demote", "bucket win rate is below the current promising gate"

    concerns: list[str] = []
    if positive_rate is None:
        concerns.append("bucket evidence is missing")
    elif positive_rate < 0.65:
        concerns.append("bucket breadth is near the 60% gate")

    adjacent_health = _adjacent_horizon_health(target, results_by_cell)
    if adjacent_health == "weak":
        concerns.append("adjacent horizon evidence is weak")
    elif adjacent_health == "mixed":
        concerns.append("adjacent horizon evidence is mixed")
    elif adjacent_health == "unknown":
        concerns.append("adjacent horizon evidence is missing")

    if cost_multiple is None:
        concerns.append("cost sensitivity is unknown")

    if target_attribution is None or not target_attribution.tickers:
        concerns.append("ticker concentration is unavailable")
    else:
        top_share = _top_ticker_share(target_attribution)
        if top_share is not None and top_share >= 0.20:
            concerns.append("ticker concentration is high")
    if concerns:
        return "watch", "; ".join(concerns[:3])

    return (
        "continue",
        "edge is broad, cost cushion is usable, and adjacent evidence supports it",
    )


def _temporal_concentration_text(result: ResearchResultRow) -> str:
    if not result.walk_forward_buckets:
        return "unknown; no walk-forward bucket evidence"

    positive_edges = [
        bucket.net_difference_vs_baseline
        for bucket in result.walk_forward_buckets
        if bucket.net_difference_vs_baseline > 0
    ]
    total_positive_edge = sum(positive_edges)
    if total_positive_edge <= 0:
        return "no positive bucket edge"

    top_bucket = max(
        result.walk_forward_buckets,
        key=lambda bucket: bucket.net_difference_vs_baseline,
    )
    share = top_bucket.net_difference_vs_baseline / total_positive_edge
    return (
        "largest positive bucket contributes "
        f"{share:.1%} of positive bucket edge "
        f"({_date_range_text(top_bucket)})"
    )


def _deep_dive_adjacent_summary(
    target: ResearchResultRow,
    results_by_cell: Mapping[tuple[str, int], ResearchResultRow],
) -> str:
    adjacent = [
        results_by_cell.get((target.base_hypothesis_key, horizon))
        for horizon in _adjacent_horizons(target.horizon_days or 0)
    ]
    adjacent_results = [item for item in adjacent if item is not None]
    if not adjacent_results:
        return "no adjacent horizon rows found"
    return "; ".join(
        f"h{result.horizon_days} {_review_verdict(result)} "
        f"edge {_signed_percent(result.net_difference_vs_baseline)}"
        for result in adjacent_results
    )


def _deep_dive_year_driver_table(result: ResearchResultRow) -> str:
    if not result.walk_forward_buckets:
        return "No walk-forward bucket windows found."

    by_year: dict[str, list[WalkForwardBucket]] = {}
    for bucket in result.walk_forward_buckets:
        by_year.setdefault(bucket.year, []).append(bucket)

    rows: list[tuple[str, ...]] = []
    for year, buckets in sorted(by_year.items()):
        positive = sum(1 for bucket in buckets if bucket.sign == "+")
        average_edge = _mean(
            [bucket.net_difference_vs_baseline for bucket in buckets]
        )
        rows.append(
            (
                year,
                f"{positive}/{len(buckets)} ({positive / len(buckets):.1%})",
                _signed_percent(average_edge),
                "".join(bucket.sign for bucket in buckets),
            )
        )

    return _table(("Year", "Positive buckets", "Average edge", "Pattern"), rows)


def _weakest_bucket_table(result: ResearchResultRow) -> str:
    if not result.walk_forward_buckets:
        return "No walk-forward bucket windows found."

    weakest = sorted(
        result.walk_forward_buckets,
        key=lambda bucket: bucket.net_difference_vs_baseline,
    )[:5]
    rows = [
        (
            bucket.test_start or "unknown",
            bucket.test_end or "unknown",
            _signed_percent(bucket.net_difference_vs_baseline),
            bucket.sign,
        )
        for bucket in weakest
    ]
    return _table(("Test start", "Test end", "Edge", "Sign"), rows)


def _deep_dive_adjacent_table(
    target: ResearchResultRow,
    results_by_cell: Mapping[tuple[str, int], ResearchResultRow],
) -> str:
    rows: list[tuple[str, ...]] = []
    for horizon in PROMISING_DEEP_DIVE_COMPARISON_HORIZONS:
        result = results_by_cell.get((target.base_hypothesis_key, horizon))
        if result is None:
            rows.append((f"h{horizon}", "pending", "n/a", "n/a", "n/a"))
            continue
        rows.append(
            (
                f"h{horizon}",
                _review_verdict(result),
                _signed_percent(result.net_difference_vs_baseline),
                _positive_bucket_text(result),
                _cost_sensitivity_text(result),
            )
        )
    return _table(("Horizon", "Verdict", "Edge", "Buckets", "Cost"), rows)


def _momentum_overlap_proxy_lines(
    results_by_cell: Mapping[tuple[str, int], ResearchResultRow],
) -> list[str]:
    lines: list[str] = []
    for base_key in PROMISING_DEEP_DIVE_MOMENTUM_BASE_KEYS:
        if lines:
            lines.append("")
        lines.append(f"{base_key}:")
        for horizon in PROMISING_DEEP_DIVE_COMPARISON_HORIZONS:
            lines.append(
                "- h"
                f"{horizon}: "
                f"{_compact_horizon_cell(results_by_cell.get((base_key, horizon)))}"
            )
    return lines


def _selection_attribution_for(
    selection_attributions: Sequence[SelectionAttributionSet],
    *,
    base_key: str,
    horizon: int,
) -> SelectionAttributionSet | None:
    for attribution in selection_attributions:
        if (
            attribution.base_hypothesis_key == base_key
            and attribution.horizon_days == horizon
        ):
            return attribution
    return None


def _selected_ticker_lines(
    target_attribution: SelectionAttributionSet | None,
    selection_attributions: Sequence[SelectionAttributionSet],
) -> list[str]:
    if target_attribution is None or not target_attribution.tickers:
        return [
            "- not_available: stored selection attribution is not available in "
            "current report rows."
        ]

    return [
        (
            "- source: reconstructed read-only from persisted feature values, "
            "forward-return labels, and walk-forward windows."
        ),
        f"- concentration: {_ticker_concentration_text(target_attribution)}",
        "- same-horizon momentum overlap:",
        *_momentum_selection_overlap_lines(target_attribution, selection_attributions),
        "",
        _selected_ticker_table(target_attribution.tickers),
    ]


def _selected_ticker_table(tickers: Sequence[TickerAttribution]) -> str:
    if not tickers:
        return "No selected ticker attribution rows."

    total = _selected_observation_total(tickers)
    rows = [
        (
            ticker.ticker,
            str(ticker.selected_observations),
            _share_text(ticker.selected_observations, total),
            str(ticker.selected_windows),
            _signed_percent(ticker.mean_realized_return),
            ticker.positive_window_ratio,
            _signed_percent(ticker.mean_window_net_difference_when_selected),
        )
        for ticker in _top_selected_tickers(tickers, limit=10)
    ]
    return _table(
        (
            "Ticker",
            "Selected obs",
            "Share",
            "Windows",
            "Mean future return",
            "Positive windows",
            "Mean selected-window diff",
        ),
        rows,
    )


def _ticker_concentration_text(
    target_attribution: SelectionAttributionSet | None,
) -> str:
    if target_attribution is None or not target_attribution.tickers:
        return "unavailable until selected-ticker attribution is reconstructed"

    tickers = target_attribution.tickers
    total = _selected_observation_total(tickers)
    if total == 0:
        return "unavailable; selected observation count is zero"

    ranked = _top_selected_tickers(tickers, limit=len(tickers))
    top = ranked[0]
    top_five_count = sum(ticker.selected_observations for ticker in ranked[:5])
    hhi = sum((ticker.selected_observations / total) ** 2 for ticker in tickers)
    effective_count = 1 / hhi if hhi > 0 else 0.0
    return (
        f"top ticker {top.ticker} is "
        f"{top.selected_observations}/{total} selections "
        f"({_share_text(top.selected_observations, total)}); "
        f"top 5 are {top_five_count}/{total} "
        f"({_share_text(top_five_count, total)}); "
        f"HHI {hhi:.3f}, effective tickers {effective_count:.1f}"
    )


def _momentum_selection_overlap_summary(
    target_attribution: SelectionAttributionSet | None,
    selection_attributions: Sequence[SelectionAttributionSet],
) -> str:
    if target_attribution is None or not target_attribution.tickers:
        return "true ticker overlap is unavailable"

    stats = _momentum_selection_overlap_stats(target_attribution, selection_attributions)
    if not stats:
        return "same-horizon momentum attribution is unavailable"
    if all(item[0] == item[1] and item[2] >= 0.999 for item in stats):
        return "same-horizon momentum selections fully overlap target selected tickers"
    return "same-horizon momentum selected-ticker overlap is reconstructed below"


def _momentum_selection_overlap_lines(
    target_attribution: SelectionAttributionSet,
    selection_attributions: Sequence[SelectionAttributionSet],
) -> list[str]:
    lines: list[str] = []
    for base_key in PROMISING_DEEP_DIVE_MOMENTUM_BASE_KEYS:
        comparison = _selection_attribution_for(
            selection_attributions,
            base_key=base_key,
            horizon=PROMISING_DEEP_DIVE_HORIZON_DAYS,
        )
        if comparison is None or not comparison.tickers:
            lines.append(f"- {base_key}__h252: attribution unavailable")
            continue
        lines.append(
            "- "
            f"{comparison.hypothesis_key}: "
            f"{_selection_overlap_text(target_attribution, comparison)}"
        )
    return lines


def _selection_overlap_text(
    target_attribution: SelectionAttributionSet,
    comparison: SelectionAttributionSet,
) -> str:
    target_by_ticker = {ticker.ticker: ticker for ticker in target_attribution.tickers}
    comparison_tickers = {ticker.ticker for ticker in comparison.tickers}
    overlap = set(target_by_ticker) & comparison_tickers
    total = _selected_observation_total(target_attribution.tickers)
    overlap_observations = sum(
        target_by_ticker[ticker].selected_observations
        for ticker in overlap
    )
    top_overlaps = [
        ticker
        for ticker in _top_selected_tickers(target_attribution.tickers, limit=5)
        if ticker.ticker in overlap
    ]
    names = ", ".join(ticker.ticker for ticker in top_overlaps) or "none in top 5"
    return (
        f"{len(overlap)}/{len(target_by_ticker)} target tickers overlap, "
        f"covering {_share_text(overlap_observations, total)} of target "
        f"selected observations; top overlaps: {names}"
    )


def _momentum_selection_overlap_stats(
    target_attribution: SelectionAttributionSet,
    selection_attributions: Sequence[SelectionAttributionSet],
) -> list[tuple[int, int, float]]:
    stats: list[tuple[int, int, float]] = []
    target_by_ticker = {ticker.ticker: ticker for ticker in target_attribution.tickers}
    total = _selected_observation_total(target_attribution.tickers)
    if not target_by_ticker or total <= 0:
        return stats
    for base_key in PROMISING_DEEP_DIVE_MOMENTUM_BASE_KEYS:
        comparison = _selection_attribution_for(
            selection_attributions,
            base_key=base_key,
            horizon=PROMISING_DEEP_DIVE_HORIZON_DAYS,
        )
        if comparison is None or not comparison.tickers:
            continue
        overlap = set(target_by_ticker) & {ticker.ticker for ticker in comparison.tickers}
        overlap_observations = sum(
            target_by_ticker[ticker].selected_observations
            for ticker in overlap
        )
        stats.append(
            (
                len(overlap),
                len(target_by_ticker),
                overlap_observations / total,
            )
        )
    return stats


def _exposure_note_lines(
    target_attribution: SelectionAttributionSet | None,
    selection_attributions: Sequence[SelectionAttributionSet],
) -> list[str]:
    if target_attribution is None or not target_attribution.tickers:
        return [
            "- high dollar volume can be size, liquidity, or mega-cap exposure; "
            "treat this as a baseline/control candidate until ticker "
            "attribution exists."
        ]

    lines = [
        "- selected-ticker attribution is now available; concentration is broad "
        "across large/liquid names rather than driven by one ticker."
    ]
    stats = _momentum_selection_overlap_stats(target_attribution, selection_attributions)
    if stats and all(item[0] == item[1] and item[2] >= 0.999 for item in stats):
        lines.append(
            "- same-horizon momentum selected sets fully overlap the target "
            "ticker set, so independence from momentum is not established."
        )
    else:
        lines.append(
            "- high dollar volume can still be size, liquidity, or mega-cap "
            "exposure; treat this as a baseline/control candidate until richer "
            "controls exist."
        )
    return lines


def _top_selected_tickers(
    tickers: Sequence[TickerAttribution],
    *,
    limit: int,
) -> tuple[TickerAttribution, ...]:
    return tuple(
        sorted(
            tickers,
            key=lambda ticker: (
                ticker.selected_observations,
                ticker.selected_windows,
                ticker.mean_realized_return
                if ticker.mean_realized_return is not None
                else float("-inf"),
                ticker.ticker,
            ),
            reverse=True,
        )[:limit]
    )


def _selected_observation_total(tickers: Sequence[TickerAttribution]) -> int:
    return sum(ticker.selected_observations for ticker in tickers)


def _top_ticker_share(
    target_attribution: SelectionAttributionSet,
) -> float | None:
    if not target_attribution.tickers:
        return None
    total = _selected_observation_total(target_attribution.tickers)
    if total <= 0:
        return None
    top = _top_selected_tickers(target_attribution.tickers, limit=1)[0]
    return top.selected_observations / total


def _share_text(count: int, total: int) -> str:
    if total <= 0:
        return "n/a"
    return f"{count / total:.1%}"


def _compact_horizon_cell(result: ResearchResultRow | None) -> str:
    if result is None:
        return "pending"
    return (
        f"{_review_verdict(result)}, "
        f"edge {_signed_percent(result.net_difference_vs_baseline)}, "
        f"buckets {_positive_bucket_text(result)}"
    )


def _date_range_text(bucket: WalkForwardBucket) -> str:
    start = bucket.test_start or "unknown"
    end = bucket.test_end or "unknown"
    return f"{start} to {end}"


def _promising_results(
    results: Sequence[ResearchResultRow],
) -> list[ResearchResultRow]:
    return sorted(
        (result for result in results if result.verdict == "promising"),
        key=lambda result: (
            result.base_hypothesis_key,
            -1 if result.horizon_days is None else result.horizon_days,
        ),
    )


def _results_by_cell(
    results: Sequence[ResearchResultRow],
) -> dict[tuple[str, int], ResearchResultRow]:
    return {
        (result.base_hypothesis_key, result.horizon_days): result
        for result in results
        if result.horizon_days is not None
    }


def _positive_bucket_text(result: ResearchResultRow) -> str:
    if not result.walk_forward_buckets:
        return "n/a"
    positive, total = _positive_bucket_counts(result)
    return f"{positive}/{total} ({positive / total:.1%})"


def _positive_bucket_counts(result: ResearchResultRow) -> tuple[int, int]:
    positive = sum(1 for bucket in result.walk_forward_buckets if bucket.sign == "+")
    return positive, len(result.walk_forward_buckets)


def _adjacent_horizon_text(
    result: ResearchResultRow,
    results_by_cell: Mapping[tuple[str, int], ResearchResultRow],
) -> str:
    if result.horizon_days is None:
        return "n/a"
    adjacent_horizons = _adjacent_horizons(result.horizon_days)
    if not adjacent_horizons:
        return "n/a"
    parts: list[str] = []
    for horizon in adjacent_horizons:
        adjacent = results_by_cell.get((result.base_hypothesis_key, horizon))
        if adjacent is None:
            parts.append(f"h{horizon} pending")
            continue
        parts.append(f"h{horizon} {_review_verdict(adjacent)}")
    return ", ".join(parts)


def _review_verdict(result: ResearchResultRow) -> str:
    if result.verdict in {"accepted", "promising", "running", "untested"}:
        return result.verdict
    return f"{result.verdict}:{result.failure_reason}"


def _adjacent_horizons(horizon: int) -> tuple[int, ...]:
    horizons = tuple(CANONICAL_HORIZONS)
    if horizon not in horizons:
        return ()
    index = horizons.index(horizon)
    adjacent: list[int] = []
    if index > 0:
        adjacent.append(horizons[index - 1])
    if index + 1 < len(horizons):
        adjacent.append(horizons[index + 1])
    return tuple(adjacent)


def _label_scramble_text(result: ResearchResultRow) -> str:
    if result.label_scramble_pass is False:
        return "fail"
    if result.label_scramble_pass is None:
        return "missing"
    if result.label_scramble_p_value is None:
        return "pass p=n/a"
    if result.label_scramble_alpha is None:
        return f"pass p={result.label_scramble_p_value:.4f}"
    return (
        f"pass p={result.label_scramble_p_value:.4f} "
        f"<= {result.label_scramble_alpha:.4f}"
    )


def _cost_sensitivity_text(result: ResearchResultRow) -> str:
    multiple = _cost_edge_multiple(result)
    if multiple is None:
        return "unknown"
    if multiple >= 5:
        return f"low ({multiple:.1f}x current cost)"
    if multiple >= 1:
        return f"medium ({multiple:.1f}x current cost)"
    if multiple < 0:
        return "high (negative edge)"
    return f"high ({multiple:.1f}x current cost)"


def _cost_edge_multiple(result: ResearchResultRow) -> float | None:
    if result.net_difference_vs_baseline is None or result.round_trip_cost_bps is None:
        return None
    cost_fraction = result.round_trip_cost_bps / 10_000.0
    if cost_fraction <= 0:
        return None
    return result.net_difference_vs_baseline / cost_fraction


def _promising_recommendation(
    result: ResearchResultRow,
    results_by_cell: Mapping[tuple[str, int], ResearchResultRow],
) -> tuple[str, str]:
    edge = result.net_difference_vs_baseline
    positive_rate = _positive_bucket_rate(result)
    cost_multiple = _cost_edge_multiple(result)
    adjacent_health = _adjacent_horizon_health(result, results_by_cell)

    if result.label_scramble_p_value is None:
        return "watch", "label-scramble p-value is missing from the report row"
    if edge is None:
        return "demote", "edge versus baseline is missing"
    if cost_multiple is not None and cost_multiple < 1:
        return "demote", "edge is smaller than one current round-trip cost"
    if positive_rate is not None and positive_rate < 0.60:
        return "demote", "bucket win rate is below the current promising gate"

    if (
        edge >= 0.01
        and (cost_multiple is None or cost_multiple >= 5)
        and adjacent_health != "weak"
    ):
        return (
            "deep_dive",
            "large edge with usable cost cushion; inspect drivers and replay evidence",
        )

    concerns: list[str] = []
    if positive_rate is None:
        concerns.append("bucket evidence is missing")
    elif positive_rate < 0.65:
        concerns.append("bucket win rate is near the 60% gate")
    if edge < 0.005:
        concerns.append("edge is small")
    if adjacent_health == "weak":
        concerns.append("adjacent horizons are weak")
    elif adjacent_health == "mixed":
        concerns.append("adjacent horizons are mixed")
    if cost_multiple is None:
        concerns.append("cost sensitivity is unknown")

    if not concerns:
        concerns.append("passed gates but still needs operator review")
    return "watch", "; ".join(concerns[:2])


def _positive_bucket_rate(result: ResearchResultRow) -> float | None:
    if not result.walk_forward_buckets:
        return None
    positive, total = _positive_bucket_counts(result)
    return positive / total


def _adjacent_horizon_health(
    result: ResearchResultRow,
    results_by_cell: Mapping[tuple[str, int], ResearchResultRow],
) -> str:
    if result.horizon_days is None:
        return "unknown"
    adjacent = [
        results_by_cell.get((result.base_hypothesis_key, horizon))
        for horizon in _adjacent_horizons(result.horizon_days)
    ]
    adjacent_results = [item for item in adjacent if item is not None]
    if not adjacent_results:
        return "unknown"
    if any(item.verdict in {"accepted", "promising"} for item in adjacent_results):
        return "supportive"
    if any(
        item.net_difference_vs_baseline is None
        or item.net_difference_vs_baseline <= 0
        for item in adjacent_results
    ):
        return "weak"
    return "mixed"


def _reason_table(results: Sequence[ResearchResultRow]) -> str:
    reasons = Counter(
        result.failure_reason
        for result in results
        if result.failure_reason not in {"passed", "not_run"}
    )
    if not reasons:
        return "No rejection or failure reasons found."
    return _table(
        ("Reason", "Count"),
        [(reason, str(count)) for reason, count in sorted(reasons.items())],
    )


def _reason_glossary(results: Sequence[ResearchResultRow]) -> str:
    reasons = sorted(
        {
            result.failure_reason
            for result in results
            if result.failure_reason != "passed"
        }
    )
    if not reasons:
        return "No failure or navigation reasons to explain."
    rows = [(reason, _reason_explanation(reason)) for reason in reasons]
    return _table(("Reason", "Meaning"), rows)


def _reason_explanation(reason: str) -> str:
    explanations = {
        "backtest_failed": (
            "The falsifier execution failed before durable accepted evidence "
            "could be written."
        ),
        "baseline_failed": (
            "The strategy did not beat the equal-weight universe baseline after "
            "costs for the tested horizon."
        ),
        "baseline_missing": (
            "The stored backtest row did not include enough baseline evidence "
            "to support a claim."
        ),
        "insufficient_data": (
            "The run ended as a terminal no-claim result because too few usable "
            "feature, label, or walk-forward rows were available."
        ),
        "label_scramble_failed": (
            "Randomly reassigned labels produced results too close to the "
            "observed result, so the apparent signal is not robust evidence."
        ),
        "not_run": (
            "The hypothesis exists in the registry but no linked falsifier "
            "evaluation is available yet."
        ),
        "running": "The latest linked backtest is still running.",
        "walk_forward_unstable": (
            "The strategy did not beat the baseline in enough walk-forward "
            "windows, so performance was not stable across time."
        ),
    }
    if reason.startswith("unsupported_status:"):
        return "The linked backtest has a status this v0 report does not recognize."
    return explanations.get(reason, "Inspect the linked backtest for this stored reason.")


def _untested_table(results: Sequence[ResearchResultRow]) -> str:
    untested = [result for result in results if result.verdict == "untested"]
    if not untested:
        return "No untested hypotheses."
    return _table(
        ("Hypothesis", "Family", "Feature", "Direction"),
        [
            (
                result.hypothesis_key,
                result.family,
                result.feature_name,
                result.selection_direction,
            )
            for result in untested
        ],
    )


def _suggestion_lines(results: Sequence[ResearchResultRow]) -> list[str]:
    suggestions: list[str] = []
    positive_scramble_failures = [
        result
        for result in results
        if result.failure_reason == "label_scramble_failed"
        and result.net_difference_vs_baseline is not None
        and result.net_difference_vs_baseline > 0
    ]
    for result in positive_scramble_failures[:3]:
        suggestions.append(
            "- Try canonical neighboring horizons for "
            f"`{result.hypothesis_key}`; it beat baseline but failed "
            f"label-scramble at {_int_or_na(result.horizon_days)} sessions."
        )

    insufficient = [
        result for result in results if result.verdict == "insufficient_data"
    ]
    if insufficient:
        suggestions.append(
            "- Backfill or materialize missing inputs for insufficient-data "
            "hypotheses: "
            + ", ".join(result.hypothesis_key for result in insufficient[:5])
            + "."
        )

    untested = [result for result in results if result.verdict == "untested"]
    if untested:
        suggestions.append(
            "- Materialize or run the falsifier for untested hypotheses: "
            + ", ".join(result.hypothesis_key for result in untested[:5])
            + "."
        )

    if not suggestions:
        suggestions.append(
            "- No automatic next test is obvious from v0 summary rows; inspect "
            "the weakest rejected result before adding hypotheses."
        )
    return suggestions


def _count_verdict(results: Sequence[ResearchResultRow], verdict: str) -> int:
    return sum(1 for result in results if result.verdict == verdict)


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ResearchResultsError(f"{name} must be an object")
    return value


def _required_mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ResearchResultsError(f"{name} must be an object")
    return value


def _required_str(row: Mapping[str, Any], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ResearchResultsError(f"{key} must be a non-empty string")
    return value.strip()


def _optional_str(value: object, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ResearchResultsError(f"{name} must be a string")
    stripped = value.strip()
    return stripped or None


def _optional_int(value: object, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ResearchResultsError(f"{name} must be an integer")
    if value < 0:
        raise ResearchResultsError(f"{name} must be non-negative")
    return value


def _optional_float(value: object, name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ResearchResultsError(f"{name} must be numeric")
    return float(value)


def _optional_bool(value: object, name: str) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ResearchResultsError(f"{name} must be a boolean")
    return value


def _parameter_str(parameters: Mapping[str, Any], key: str) -> str | None:
    return _optional_str(parameters.get(key), key)


def _selection_direction(value: str) -> str:
    if value not in {"high", "low"}:
        raise ResearchResultsError("selection_direction must be high or low")
    return value


def _percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.4f}%"


def _signed_percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:+.4f}%"


def _int_or_na(value: int | None) -> str:
    return "n/a" if value is None else str(value)


def _list_text(values: Sequence[str]) -> str:
    return ", ".join(values) if values else "none"


def _horizon_text(values: Sequence[int]) -> str:
    if not values:
        return "none"
    return ", ".join(str(value) for value in values) + " trading sessions"


def _mean(values: Sequence[float]) -> float:
    if not values:
        raise ResearchResultsError("mean requires at least one value")
    return sum(values) / len(values)


def _family_key(value: str) -> tuple[int, str]:
    order = {"price": 0, "fundamentals": 1, "unknown": 2}
    return (order.get(value, 99), value)


def _verdict_key(value: str) -> tuple[int, str]:
    order = {
        "accepted": 0,
        "promising": 1,
        "rejected": 2,
        "insufficient_data": 3,
        "failed": 4,
        "running": 5,
        "untested": 6,
    }
    return (order.get(value, 99), value)


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    header_line = "| " + " | ".join(headers) + " |"
    separator = "| " + " | ".join("---" for _ in headers) + " |"
    row_lines = ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join((header_line, separator, *row_lines))
