"""Read-only research results report over persisted hypothesis evidence."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from silver.features.candidate_pack import (
    DEFAULT_CANDIDATE_CONFIG_PATH,
    FUNDAMENTAL_MATERIALIZERS,
    FeatureCandidate,
    load_feature_candidates,
)


class ResearchResultsError(ValueError):
    """Raised when persisted research results cannot be summarized."""


class ResearchResultsJsonClient(Protocol):
    """Minimal read-only JSON query client used by the report loader."""

    def fetch_json(self, sql: str) -> Any:
        """Return one decoded JSON value for a SQL query."""
        ...


@dataclass(frozen=True, slots=True)
class ResearchResultRow:
    """One hypothesis plus its latest linked falsifier evidence."""

    hypothesis_key: str
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
    backtest_run_id: int | None
    backtest_run_key: str | None
    model_run_key: str | None
    scored_test_dates: int | None

    @property
    def tested(self) -> bool:
        return self.backtest_run_id is not None


@dataclass(frozen=True, slots=True)
class ResearchResultsReport:
    """Complete operator-facing research results rollup."""

    results: tuple[ResearchResultRow, ...]

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
    return ResearchResultsReport(
        results=tuple(
            _research_result_row(raw, candidates_by_key)
            for raw in payload
        )
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
    candidate = candidates_by_key.get(hypothesis_key)
    backtest_parameters = _mapping(row.get("backtest_parameters"), "backtest_parameters")
    model_parameters = _mapping(row.get("model_parameters"), "model_parameters")
    metrics = _mapping(row.get("backtest_metrics"), "backtest_metrics")
    baseline_metrics = _mapping(row.get("baseline_metrics"), "baseline_metrics")

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
        backtest_run_id=backtest_run_id,
        backtest_run_key=_optional_str(row.get("backtest_run_key"), "backtest_run_key"),
        model_run_key=_optional_str(row.get("model_run_key"), "model_run_key"),
        scored_test_dates=_optional_int(
            metrics.get("scored_test_dates"),
            "scored_test_dates",
        ),
    )


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


def _int_or_na(value: int | None) -> str:
    return "n/a" if value is None else str(value)


def _list_text(values: Sequence[str]) -> str:
    return ", ".join(values) if values else "none"


def _horizon_text(values: Sequence[int]) -> str:
    if not values:
        return "none"
    return ", ".join(str(value) for value in values) + " trading sessions"


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
