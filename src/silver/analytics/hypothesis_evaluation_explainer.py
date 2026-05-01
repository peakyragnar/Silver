"""Read-only explanations for persisted hypothesis falsifier evaluations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol


DEFAULT_MIN_POSITIVE_WINDOW_RATE = 0.6


class HypothesisEvaluationExplanationError(ValueError):
    """Raised when a persisted hypothesis evaluation cannot be explained."""


class HypothesisEvaluationJsonClient(Protocol):
    """Minimal read-only JSON query client used by the explainer."""

    def fetch_json(self, sql: str) -> Any:
        """Return one decoded JSON value for a SQL query."""
        ...


@dataclass(frozen=True, slots=True)
class EvaluationIdentity:
    """Stable identity fields for a hypothesis/backtest/model explanation."""

    backtest_run_id: int
    backtest_run_key: str
    backtest_name: str
    backtest_status: str
    model_run_id: int
    model_run_key: str
    model_status: str
    model_code_git_sha: str
    model_feature_set_hash: str
    model_random_seed: int
    model_training_start_date: date
    model_training_end_date: date
    model_test_start_date: date
    model_test_end_date: date
    universe_name: str
    horizon_days: int
    target_kind: str
    strategy: str
    selection_direction: str
    label_scramble_pass: bool | None
    multiple_comparisons_correction: str | None
    cost_assumptions: Mapping[str, Any]
    model_available_at_policy_versions: Mapping[str, Any]
    model_input_fingerprints: Mapping[str, Any]
    hypothesis_key: str | None = None
    hypothesis_name: str | None = None
    hypothesis_status: str | None = None
    hypothesis_thesis: str | None = None
    hypothesis_signal_name: str | None = None
    hypothesis_mechanism: str | None = None
    evaluation_status: str | None = None
    failure_reason: str | None = None
    evaluation_notes: str | None = None


@dataclass(frozen=True, slots=True)
class WalkForwardWindowExplanation:
    """One persisted walk-forward window from a falsifier backtest."""

    split_index: int
    test_start: date
    test_end: date
    strategy_net_return: float | None
    baseline_net_return: float | None
    net_difference_vs_baseline: float | None
    scored_dates: int

    @property
    def is_positive(self) -> bool:
        return (
            self.net_difference_vs_baseline is not None
            and self.net_difference_vs_baseline > 0
        )

    @property
    def test_range(self) -> str:
        return _date_range(self.test_start, self.test_end)


@dataclass(frozen=True, slots=True)
class TickerAttribution:
    """Selected-row ticker attribution reconstructed from persisted inputs."""

    ticker: str
    selected_observations: int
    selected_windows: int
    positive_windows_selected: int
    negative_windows_selected: int
    mean_realized_return: float | None
    mean_window_net_difference_when_selected: float | None

    @property
    def positive_window_ratio(self) -> str:
        return f"{self.positive_windows_selected}/{self.selected_windows}"


@dataclass(frozen=True, slots=True)
class HypothesisEvaluationExplanation:
    """Complete read-only explanation for one persisted evaluation result."""

    identity: EvaluationIdentity
    metrics: Mapping[str, Any]
    baseline_metrics: Mapping[str, Any]
    label_scramble_metrics: Mapping[str, Any]
    metrics_by_regime: Mapping[str, Any]
    walk_forward_windows: tuple[WalkForwardWindowExplanation, ...]
    ticker_attribution: tuple[TickerAttribution, ...]

    @property
    def scored_windows(self) -> int:
        return len(
            [
                window
                for window in self.walk_forward_windows
                if window.net_difference_vs_baseline is not None
            ]
        )

    @property
    def positive_windows(self) -> int:
        return sum(1 for window in self.walk_forward_windows if window.is_positive)

    @property
    def positive_window_rate(self) -> float | None:
        if self.scored_windows == 0:
            return None
        return self.positive_windows / self.scored_windows

    @property
    def mean_window_net_difference(self) -> float | None:
        values = [
            window.net_difference_vs_baseline
            for window in self.walk_forward_windows
            if window.net_difference_vs_baseline is not None
        ]
        if not values:
            return None
        return sum(values) / len(values)

    @property
    def strongest_windows(self) -> tuple[WalkForwardWindowExplanation, ...]:
        return tuple(
            sorted(
                _scored_windows(self.walk_forward_windows),
                key=lambda window: (
                    window.net_difference_vs_baseline
                    if window.net_difference_vs_baseline is not None
                    else float("-inf")
                ),
                reverse=True,
            )
        )

    @property
    def weakest_windows(self) -> tuple[WalkForwardWindowExplanation, ...]:
        return tuple(
            sorted(
                _scored_windows(self.walk_forward_windows),
                key=lambda window: (
                    window.net_difference_vs_baseline
                    if window.net_difference_vs_baseline is not None
                    else float("inf")
                ),
            )
        )

    @property
    def strongest_tickers(self) -> tuple[TickerAttribution, ...]:
        return tuple(
            sorted(
                self.ticker_attribution,
                key=lambda ticker: (
                    ticker.mean_realized_return
                    if ticker.mean_realized_return is not None
                    else float("-inf"),
                    ticker.ticker,
                ),
                reverse=True,
            )
        )

    @property
    def weakest_tickers(self) -> tuple[TickerAttribution, ...]:
        return tuple(
            sorted(
                self.ticker_attribution,
                key=lambda ticker: (
                    ticker.mean_realized_return
                    if ticker.mean_realized_return is not None
                    else float("inf"),
                    ticker.ticker,
                ),
            )
        )


def load_hypothesis_evaluation_explanation(
    client: HypothesisEvaluationJsonClient,
    *,
    backtest_run_id: int | None = None,
    hypothesis_key: str | None = None,
) -> HypothesisEvaluationExplanation:
    """Load a read-only explanation by backtest id or latest hypothesis key."""

    if (backtest_run_id is None) == (hypothesis_key is None):
        raise HypothesisEvaluationExplanationError(
            "supply exactly one of backtest_run_id or hypothesis_key"
        )

    sql = _explanation_sql(
        backtest_run_id=backtest_run_id,
        hypothesis_key=hypothesis_key,
    )
    payload = client.fetch_json(sql)
    if payload is None:
        identity = (
            f"backtest_run_id {backtest_run_id}"
            if backtest_run_id is not None
            else f"hypothesis_key {hypothesis_key}"
        )
        raise HypothesisEvaluationExplanationError(f"{identity} was not found")
    if not isinstance(payload, Mapping):
        raise HypothesisEvaluationExplanationError(
            "explanation query returned non-object JSON"
        )

    return HypothesisEvaluationExplanation(
        identity=_identity(payload.get("identity")),
        metrics=_mapping(payload.get("metrics"), "metrics"),
        baseline_metrics=_mapping(payload.get("baseline_metrics"), "baseline_metrics"),
        label_scramble_metrics=_mapping(
            payload.get("label_scramble_metrics"),
            "label_scramble_metrics",
        ),
        metrics_by_regime=_mapping(payload.get("metrics_by_regime"), "metrics_by_regime"),
        walk_forward_windows=tuple(
            _walk_forward_window(row)
            for row in _list(payload.get("walk_forward_windows"), "walk_forward_windows")
        ),
        ticker_attribution=tuple(
            _ticker_attribution(row)
            for row in _list(payload.get("ticker_attribution"), "ticker_attribution")
        ),
    )


def render_hypothesis_evaluation_explanation(
    explanation: HypothesisEvaluationExplanation,
    *,
    top: int = 5,
) -> str:
    """Render an operator-facing explanation of a persisted evaluation."""

    if isinstance(top, bool) or top < 1:
        raise HypothesisEvaluationExplanationError("top must be a positive integer")

    identity = explanation.identity
    lines = [
        "Falsifier evaluation explanation",
        f"Verdict: {_verdict(identity)}",
        "",
        "Question:",
        f"- Strategy: {identity.strategy} ({_selection_direction_label(identity)})",
        f"- Hypothesis: {identity.hypothesis_key or 'not linked'}",
        f"- Universe: {identity.universe_name}",
        f"- Horizon: {identity.horizon_days} trading sessions",
        f"- Target: {identity.target_kind}",
        "",
        "Identity:",
        f"- model_run_id: {identity.model_run_id}",
        f"- backtest_run_id: {identity.backtest_run_id}",
        f"- code_git_sha: {_short_token(identity.model_code_git_sha)}",
        f"- feature_set_hash: {_short_token(identity.model_feature_set_hash)}",
        f"- input fingerprint: {_input_fingerprint(identity)}",
        f"- available_at policies: {_policy_versions(identity)}",
        "",
        "Headline:",
        f"- Strategy net mean: {_percent(_strategy_net_mean(explanation))}",
        f"- Equal-weight baseline net mean: {_percent(_baseline_net_mean(explanation))}",
        f"- Difference vs baseline: {_percent(_baseline_difference(explanation))}",
        f"- Scored test dates: {_metric_int(explanation.metrics, 'scored_test_dates')}",
        (
            f"- Walk-forward windows: {explanation.positive_windows}/"
            f"{explanation.scored_windows} positive "
            f"({_rate(explanation.positive_window_rate)})"
        ),
        (
            "- Mean window net difference vs baseline: "
            f"{_percent(explanation.mean_window_net_difference)}"
        ),
        f"- Label scramble: {_label_scramble_line(explanation)}",
        "",
        "Why This Verdict:",
        *_verdict_lines(explanation),
        "",
        "Strongest Windows:",
        _window_table(explanation.strongest_windows[:top]),
        "",
        "Weakest Windows:",
        _window_table(explanation.weakest_windows[:top]),
        "",
        "Strongest Selected Tickers:",
        _ticker_table(explanation.strongest_tickers[:top]),
        "",
        "Weakest Selected Tickers:",
        _ticker_table(explanation.weakest_tickers[:top]),
        "",
        "Regime Summary:",
        _regime_table(explanation.metrics_by_regime),
        "",
        "Attribution Note:",
        (
            "Ticker rows are selected-row attribution rebuilt from persisted "
            "feature and label rows. They show which selected tickers had the "
            "strongest or weakest realized future returns while selected; they "
            "are not a causal decomposition."
        ),
    ]
    return "\n".join(lines) + "\n"


def _identity(raw: object) -> EvaluationIdentity:
    row = _required_mapping(raw, "identity")
    return EvaluationIdentity(
        hypothesis_key=_optional_str(row.get("hypothesis_key"), "hypothesis_key"),
        hypothesis_name=_optional_str(row.get("hypothesis_name"), "hypothesis_name"),
        hypothesis_status=_optional_str(
            row.get("hypothesis_status"),
            "hypothesis_status",
        ),
        hypothesis_thesis=_optional_str(
            row.get("hypothesis_thesis"),
            "hypothesis_thesis",
        ),
        hypothesis_signal_name=_optional_str(
            row.get("hypothesis_signal_name"),
            "hypothesis_signal_name",
        ),
        hypothesis_mechanism=_optional_str(
            row.get("hypothesis_mechanism"),
            "hypothesis_mechanism",
        ),
        evaluation_status=_optional_str(
            row.get("evaluation_status"),
            "evaluation_status",
        ),
        failure_reason=_optional_str(row.get("failure_reason"), "failure_reason"),
        evaluation_notes=_optional_str(row.get("evaluation_notes"), "evaluation_notes"),
        model_run_id=_required_int(row, "model_run_id"),
        model_run_key=_required_str(row, "model_run_key"),
        model_status=_required_str(row, "model_status"),
        model_code_git_sha=_required_str(row, "model_code_git_sha"),
        model_feature_set_hash=_required_str(row, "model_feature_set_hash"),
        model_random_seed=_required_int(row, "model_random_seed"),
        model_training_start_date=_required_date(row, "model_training_start_date"),
        model_training_end_date=_required_date(row, "model_training_end_date"),
        model_test_start_date=_required_date(row, "model_test_start_date"),
        model_test_end_date=_required_date(row, "model_test_end_date"),
        model_available_at_policy_versions=_mapping(
            row.get("model_available_at_policy_versions"),
            "model_available_at_policy_versions",
        ),
        model_input_fingerprints=_mapping(
            row.get("model_input_fingerprints"),
            "model_input_fingerprints",
        ),
        backtest_run_id=_required_int(row, "backtest_run_id"),
        backtest_run_key=_required_str(row, "backtest_run_key"),
        backtest_name=_required_str(row, "backtest_name"),
        backtest_status=_required_str(row, "backtest_status"),
        universe_name=_required_str(row, "universe_name"),
        horizon_days=_required_int(row, "horizon_days"),
        target_kind=_required_str(row, "target_kind"),
        label_scramble_pass=_optional_bool(
            row.get("label_scramble_pass"),
            "label_scramble_pass",
        ),
        multiple_comparisons_correction=_optional_str(
            row.get("multiple_comparisons_correction"),
            "multiple_comparisons_correction",
        ),
        strategy=_required_str(row, "strategy"),
        selection_direction=_selection_direction(
            _required_str(row, "selection_direction")
        ),
        cost_assumptions=_mapping(row.get("cost_assumptions"), "cost_assumptions"),
    )


def _walk_forward_window(raw: object) -> WalkForwardWindowExplanation:
    row = _required_mapping(raw, "walk_forward_window")
    return WalkForwardWindowExplanation(
        split_index=_required_int(row, "split_index"),
        test_start=_required_date(row, "test_start"),
        test_end=_required_date(row, "test_end"),
        strategy_net_return=_optional_float(
            row.get("strategy_net_return"),
            "strategy_net_return",
        ),
        baseline_net_return=_optional_float(
            row.get("baseline_net_return"),
            "baseline_net_return",
        ),
        net_difference_vs_baseline=_optional_float(
            row.get("net_difference_vs_baseline"),
            "net_difference_vs_baseline",
        ),
        scored_dates=_required_int(row, "scored_dates"),
    )


def _ticker_attribution(raw: object) -> TickerAttribution:
    row = _required_mapping(raw, "ticker_attribution")
    return TickerAttribution(
        ticker=_required_str(row, "ticker").upper(),
        selected_observations=_required_int(row, "selected_observations"),
        selected_windows=_required_int(row, "selected_windows"),
        positive_windows_selected=_required_int(row, "positive_windows_selected"),
        negative_windows_selected=_required_int(row, "negative_windows_selected"),
        mean_realized_return=_optional_float(
            row.get("mean_realized_return"),
            "mean_realized_return",
        ),
        mean_window_net_difference_when_selected=_optional_float(
            row.get("mean_window_net_difference_when_selected"),
            "mean_window_net_difference_when_selected",
        ),
    )


def _explanation_sql(
    *,
    backtest_run_id: int | None,
    hypothesis_key: str | None,
) -> str:
    target_cte = _target_backtest_cte(
        backtest_run_id=backtest_run_id,
        hypothesis_key=hypothesis_key,
    )
    return f"""
WITH target_backtest AS (
{target_cte}
),
run_row AS (
    SELECT
        h.hypothesis_key,
        h.name AS hypothesis_name,
        h.status AS hypothesis_status,
        h.thesis AS hypothesis_thesis,
        h.signal_name AS hypothesis_signal_name,
        h.mechanism AS hypothesis_mechanism,
        he.evaluation_status,
        he.failure_reason,
        he.notes AS evaluation_notes,
        mr.id AS model_run_id,
        mr.model_run_key,
        mr.status AS model_status,
        mr.code_git_sha AS model_code_git_sha,
        mr.feature_set_hash AS model_feature_set_hash,
        mr.random_seed AS model_random_seed,
        mr.training_start_date AS model_training_start_date,
        mr.training_end_date AS model_training_end_date,
        mr.test_start_date AS model_test_start_date,
        mr.test_end_date AS model_test_end_date,
        mr.available_at_policy_versions AS model_available_at_policy_versions,
        mr.input_fingerprints AS model_input_fingerprints,
        br.id AS backtest_run_id,
        br.backtest_run_key,
        br.name AS backtest_name,
        br.status AS backtest_status,
        br.universe_name,
        br.horizon_days,
        br.target_kind,
        br.label_scramble_pass,
        br.multiple_comparisons_correction,
        COALESCE(br.parameters->>'strategy', mr.parameters->>'strategy') AS strategy,
        COALESCE(
            br.parameters->>'selection_direction',
            mr.parameters->>'selection_direction',
            'high'
        ) AS selection_direction,
        br.cost_assumptions,
        br.metrics,
        br.baseline_metrics,
        br.label_scramble_metrics,
        br.metrics_by_regime
    FROM target_backtest tb
    JOIN silver.backtest_runs br ON br.id = tb.backtest_run_id
    JOIN silver.model_runs mr ON mr.id = br.model_run_id
    LEFT JOIN LATERAL (
        SELECT he_inner.*
        FROM silver.hypothesis_evaluations he_inner
        WHERE he_inner.backtest_run_id = br.id
        ORDER BY he_inner.created_at DESC, he_inner.id DESC
        LIMIT 1
    ) he ON TRUE
    LEFT JOIN silver.hypotheses h ON h.id = he.hypothesis_id
),
identity_row AS (
    SELECT
        hypothesis_key,
        hypothesis_name,
        hypothesis_status,
        hypothesis_thesis,
        hypothesis_signal_name,
        hypothesis_mechanism,
        evaluation_status,
        failure_reason,
        evaluation_notes,
        model_run_id,
        model_run_key,
        model_status,
        model_code_git_sha,
        model_feature_set_hash,
        model_random_seed,
        model_training_start_date::text AS model_training_start_date,
        model_training_end_date::text AS model_training_end_date,
        model_test_start_date::text AS model_test_start_date,
        model_test_end_date::text AS model_test_end_date,
        model_available_at_policy_versions,
        model_input_fingerprints,
        backtest_run_id,
        backtest_run_key,
        backtest_name,
        backtest_status,
        universe_name,
        horizon_days,
        target_kind,
        label_scramble_pass,
        multiple_comparisons_correction,
        strategy,
        selection_direction,
        cost_assumptions
    FROM run_row
),
feature_definition AS (
    SELECT fd.id
    FROM silver.feature_definitions fd
    JOIN run_row rr ON rr.strategy = fd.name
    ORDER BY fd.version DESC
    LIMIT 1
),
walk_forward_windows AS (
    SELECT
        (item.value->>'split_index')::integer AS split_index,
        (item.value->>'test_start')::date AS test_start,
        (item.value->>'test_end')::date AS test_end,
        (item.value->>'strategy_net_return')::float8 AS strategy_net_return,
        (item.value->>'baseline_net_return')::float8 AS baseline_net_return,
        (
            item.value->>'net_difference_vs_baseline'
        )::float8 AS net_difference_vs_baseline,
        (item.value->>'scored_dates')::integer AS scored_dates
    FROM run_row rr
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(rr.metrics->'walk_forward_windows', '[]'::jsonb)
    ) AS item(value)
),
joined_rows AS (
    SELECT
        s.ticker,
        fv.asof_date,
        CASE
            WHEN rr.selection_direction = 'low' THEN -fv.value
            ELSE fv.value
        END AS rank_feature_value,
        COALESCE(
            frl.realized_excess_return,
            frl.realized_raw_return
        )::float8 AS realized_return
    FROM run_row rr
    JOIN silver.universe_membership um
      ON um.universe_name = rr.universe_name
    JOIN silver.securities s ON s.id = um.security_id
    JOIN feature_definition fd ON TRUE
    JOIN silver.feature_values fv
      ON fv.security_id = um.security_id
     AND fv.feature_definition_id = fd.id
     AND fv.asof_date >= um.valid_from
     AND (um.valid_to IS NULL OR fv.asof_date <= um.valid_to)
    JOIN silver.forward_return_labels frl
      ON frl.security_id = fv.security_id
     AND frl.label_date = fv.asof_date
     AND frl.horizon_days = rr.horizon_days
),
ranked_rows AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY asof_date
            ORDER BY rank_feature_value DESC, ticker
        ) AS rank_position,
        count(*) OVER (PARTITION BY asof_date) AS eligible_count
    FROM joined_rows
),
selected_rows AS (
    SELECT *
    FROM ranked_rows
    WHERE rank_position <= GREATEST(
        1,
        floor(eligible_count::numeric / 2)::integer
    )
),
window_selected_rows AS (
    SELECT
        sr.ticker,
        sr.realized_return,
        wf.split_index,
        wf.net_difference_vs_baseline
    FROM selected_rows sr
    JOIN walk_forward_windows wf
      ON sr.asof_date >= wf.test_start
     AND sr.asof_date <= wf.test_end
),
ticker_attribution AS (
    SELECT
        ticker,
        count(*)::integer AS selected_observations,
        count(DISTINCT split_index)::integer AS selected_windows,
        count(DISTINCT split_index) FILTER (
            WHERE net_difference_vs_baseline > 0
        )::integer AS positive_windows_selected,
        count(DISTINCT split_index) FILTER (
            WHERE net_difference_vs_baseline <= 0
        )::integer AS negative_windows_selected,
        avg(realized_return)::float8 AS mean_realized_return,
        avg(net_difference_vs_baseline)::float8
            AS mean_window_net_difference_when_selected
    FROM window_selected_rows
    GROUP BY ticker
)
SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM run_row) THEN 'null'::jsonb
    ELSE jsonb_build_object(
        'identity', (SELECT to_jsonb(identity_row) FROM identity_row),
        'metrics', (SELECT metrics FROM run_row),
        'baseline_metrics', (SELECT baseline_metrics FROM run_row),
        'label_scramble_metrics', (SELECT label_scramble_metrics FROM run_row),
        'metrics_by_regime', (SELECT metrics_by_regime FROM run_row),
        'walk_forward_windows', (
            SELECT COALESCE(
                jsonb_agg(to_jsonb(row) ORDER BY row.split_index),
                '[]'::jsonb
            )
            FROM walk_forward_windows row
        ),
        'ticker_attribution', (
            SELECT COALESCE(
                jsonb_agg(
                    to_jsonb(row)
                    ORDER BY row.mean_realized_return DESC NULLS LAST, row.ticker
                ),
                '[]'::jsonb
            )
            FROM ticker_attribution row
        )
    )
END::text;
""".strip()


def _target_backtest_cte(
    *,
    backtest_run_id: int | None,
    hypothesis_key: str | None,
) -> str:
    if backtest_run_id is not None:
        return (
            "    SELECT "
            f"{_positive_int(backtest_run_id, 'backtest_run_id')}::bigint "
            "AS backtest_run_id"
        )
    key = _required_label(hypothesis_key, "hypothesis_key")
    return f"""
    SELECT he.backtest_run_id
    FROM silver.hypotheses h
    JOIN silver.hypothesis_evaluations he ON he.hypothesis_id = h.id
    WHERE h.hypothesis_key = {_sql_literal(key)}
    ORDER BY he.created_at DESC, he.id DESC
    LIMIT 1
""".rstrip()


def _verdict(identity: EvaluationIdentity) -> str:
    status = identity.evaluation_status or identity.backtest_status
    if identity.failure_reason:
        return f"{status} ({identity.failure_reason})"
    return status


def _selection_direction_label(identity: EvaluationIdentity) -> str:
    if identity.selection_direction == "low":
        return "low values selected"
    return "high values selected"


def _strategy_net_mean(explanation: HypothesisEvaluationExplanation) -> float | None:
    return _optional_float(
        explanation.metrics.get("mean_strategy_net_horizon_return"),
        "mean_strategy_net_horizon_return",
    )


def _baseline_net_mean(explanation: HypothesisEvaluationExplanation) -> float | None:
    equal_weight = explanation.baseline_metrics.get("equal_weight_universe")
    if not isinstance(equal_weight, Mapping):
        return None
    return _optional_float(
        equal_weight.get("mean_net_horizon_return"),
        "baseline.mean_net_horizon_return",
    )


def _baseline_difference(explanation: HypothesisEvaluationExplanation) -> float | None:
    comparison = explanation.baseline_metrics.get("strategy_vs_equal_weight_universe")
    if not isinstance(comparison, Mapping):
        return None
    return _optional_float(
        comparison.get("mean_net_difference"),
        "strategy_vs_equal_weight_universe.mean_net_difference",
    )


def _label_scramble_line(explanation: HypothesisEvaluationExplanation) -> str:
    metrics = explanation.label_scramble_metrics
    status = metrics.get("status")
    pass_label = "passed" if explanation.identity.label_scramble_pass else "failed"
    p_value = _optional_float(metrics.get("p_value"), "p_value")
    alpha = _optional_float(metrics.get("alpha"), "alpha")
    if p_value is None or alpha is None:
        reason = _optional_str(metrics.get("reason"), "reason")
        if reason:
            return f"{pass_label} ({status or 'not_run'}; {reason})"
        return f"{pass_label} ({status or 'unknown'})"
    return f"{pass_label} (p={p_value:.4f}, alpha={alpha:.4f})"


def _verdict_lines(explanation: HypothesisEvaluationExplanation) -> list[str]:
    identity = explanation.identity
    lines: list[str] = []
    if identity.evaluation_status:
        if identity.failure_reason:
            lines.append(
                "The registry rejected this hypothesis because the stored failure "
                f"reason is `{identity.failure_reason}`."
            )
        else:
            lines.append(
                "The registry verdict is "
                f"`{identity.evaluation_status}` with no stored failure reason."
            )
    else:
        lines.append(
            "No hypothesis evaluation row is linked to this backtest; showing "
            "backtest evidence only."
        )

    if identity.failure_reason == "walk_forward_unstable":
        lines.append(
            "The stored walk-forward evidence shows "
            f"{explanation.positive_windows}/{explanation.scored_windows} positive "
            f"windows ({_rate(explanation.positive_window_rate)}). The default "
            "candidate-pack gate expects stable positive-window evidence, not just "
            "one favorable average."
        )
    if identity.label_scramble_pass is False:
        lines.append(
            "The label-scramble robustness check did not pass, so randomized-label "
            "evidence remains too close to the observed result."
        )
    if not lines:
        lines.append("No failure explanation was stored.")
    return [f"- {line}" for line in lines]


def _window_table(windows: Sequence[WalkForwardWindowExplanation]) -> str:
    if not windows:
        return "No scored walk-forward windows."
    rows = [
        (
            str(window.split_index),
            window.test_range,
            _percent(window.strategy_net_return),
            _percent(window.baseline_net_return),
            _percent(window.net_difference_vs_baseline),
            str(window.scored_dates),
        )
        for window in windows
    ]
    return _table(
        (
            "Split",
            "Test window",
            "Strategy net",
            "Baseline net",
            "Diff",
            "Scored dates",
        ),
        rows,
    )


def _ticker_table(tickers: Sequence[TickerAttribution]) -> str:
    if not tickers:
        return "No selected ticker attribution rows."
    rows = [
        (
            ticker.ticker,
            str(ticker.selected_observations),
            str(ticker.selected_windows),
            _percent(ticker.mean_realized_return),
            ticker.positive_window_ratio,
            _percent(ticker.mean_window_net_difference_when_selected),
        )
        for ticker in tickers
    ]
    return _table(
        (
            "Ticker",
            "Selected obs",
            "Selected windows",
            "Mean future return",
            "Positive windows",
            "Mean selected-window diff",
        ),
        rows,
    )


def _regime_table(metrics_by_regime: Mapping[str, Any]) -> str:
    if not metrics_by_regime:
        return "No regime metrics stored."
    rows: list[tuple[str, ...]] = []
    for regime_name, raw in sorted(metrics_by_regime.items()):
        if not isinstance(raw, Mapping):
            continue
        difference = raw.get("net_difference_vs_baseline")
        strategy = raw.get("strategy_net_return")
        baseline = raw.get("baseline_net_return")
        difference_mean = (
            difference.get("mean") if isinstance(difference, Mapping) else None
        )
        difference_hit_rate = (
            difference.get("hit_rate") if isinstance(difference, Mapping) else None
        )
        strategy_mean = strategy.get("mean") if isinstance(strategy, Mapping) else None
        baseline_mean = baseline.get("mean") if isinstance(baseline, Mapping) else None
        rows.append(
            (
                regime_name,
                _date_range(
                    _optional_date(raw.get("start_date"), "start_date"),
                    _optional_date(raw.get("end_date"), "end_date"),
                ),
                str(_optional_int(raw.get("sample_count"), "sample_count") or 0),
                _percent(_optional_float(strategy_mean, "strategy_mean")),
                _percent(_optional_float(baseline_mean, "baseline_mean")),
                _percent(_optional_float(difference_mean, "difference_mean")),
                _rate(_optional_float(difference_hit_rate, "difference_hit_rate")),
            )
        )
    if not rows:
        return "No valid regime metrics stored."
    return _table(
        (
            "Regime",
            "Date range",
            "Samples",
            "Strategy",
            "Baseline",
            "Diff",
            "Diff hit rate",
        ),
        rows,
    )


def _scored_windows(
    windows: Sequence[WalkForwardWindowExplanation],
) -> tuple[WalkForwardWindowExplanation, ...]:
    return tuple(
        window for window in windows if window.net_difference_vs_baseline is not None
    )


def _metric_int(metrics: Mapping[str, Any], key: str) -> str:
    value = _optional_int(metrics.get(key), key)
    return "n/a" if value is None else str(value)


def _input_fingerprint(identity: EvaluationIdentity) -> str:
    fingerprint = identity.model_input_fingerprints.get(
        "joined_feature_label_rows_sha256"
    )
    if isinstance(fingerprint, str) and fingerprint.strip():
        return _short_token(fingerprint)
    return "n/a"


def _policy_versions(identity: EvaluationIdentity) -> str:
    if not identity.model_available_at_policy_versions:
        return "none"
    return ", ".join(
        f"{key}={value}"
        for key, value in sorted(identity.model_available_at_policy_versions.items())
    )


def _short_token(value: str, length: int = 12) -> str:
    return value[:length] if len(value) > length else value


def _required_mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise HypothesisEvaluationExplanationError(f"{name} must be an object")
    return value


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise HypothesisEvaluationExplanationError(f"{name} must be an object")
    return value


def _list(value: object, name: str) -> list[object]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise HypothesisEvaluationExplanationError(f"{name} must be a list")
    return value


def _required_str(row: Mapping[str, Any], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise HypothesisEvaluationExplanationError(f"{key} must be a non-empty string")
    return value.strip()


def _optional_str(value: object, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise HypothesisEvaluationExplanationError(f"{name} must be a string")
    stripped = value.strip()
    return stripped or None


def _required_int(row: Mapping[str, Any], key: str) -> int:
    value = _optional_int(row.get(key), key)
    if value is None:
        raise HypothesisEvaluationExplanationError(f"{key} must be an integer")
    return value


def _optional_int(value: object, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise HypothesisEvaluationExplanationError(f"{name} must be an integer")
    if value < 0:
        raise HypothesisEvaluationExplanationError(f"{name} must be non-negative")
    return value


def _optional_bool(value: object, name: str) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise HypothesisEvaluationExplanationError(f"{name} must be a boolean")
    return value


def _optional_float(value: object, name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise HypothesisEvaluationExplanationError(f"{name} must be numeric")
    return float(value)


def _required_date(row: Mapping[str, Any], key: str) -> date:
    value = _optional_date(row.get(key), key)
    if value is None:
        raise HypothesisEvaluationExplanationError(f"{key} must be a date")
    return value


def _optional_date(value: object, name: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        raise HypothesisEvaluationExplanationError(f"{name} must be a date")
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value.strip())
        except ValueError as exc:
            raise HypothesisEvaluationExplanationError(
                f"{name} must use YYYY-MM-DD"
            ) from exc
    raise HypothesisEvaluationExplanationError(f"{name} must be a date")


def _selection_direction(value: str) -> str:
    if value not in {"high", "low"}:
        raise HypothesisEvaluationExplanationError(
            "selection_direction must be high or low"
        )
    return value


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise HypothesisEvaluationExplanationError(f"{name} must be a positive integer")
    return value


def _required_label(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HypothesisEvaluationExplanationError(f"{name} must be non-empty")
    return value.strip()


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.4f}%"


def _rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.3f}"


def _date_range(start: date | None, end: date | None) -> str:
    if start is None:
        return "n/a"
    if end is None or start == end:
        return start.isoformat()
    return f"{start.isoformat()} to {end.isoformat()}"


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    header_line = "| " + " | ".join(headers) + " |"
    separator = "| " + " | ".join("---" for _ in headers) + " |"
    row_lines = ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join((header_line, separator, *row_lines))
