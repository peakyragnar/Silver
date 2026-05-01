from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from silver.analytics.hypothesis_evaluation_explainer import (
    HypothesisEvaluationExplanationError,
    load_hypothesis_evaluation_explanation,
    render_hypothesis_evaluation_explanation,
)


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "explain_hypothesis_evaluation.py"


def test_render_explains_rejected_walk_forward_evaluation() -> None:
    explanation = load_hypothesis_evaluation_explanation(
        FakeJsonClient(_payload()),
        backtest_run_id=25,
    )

    rendered = render_hypothesis_evaluation_explanation(explanation, top=2)

    assert "Falsifier evaluation explanation" in rendered
    assert "Verdict: rejected (walk_forward_unstable)" in rendered
    assert "- Strategy: return_21_0 (low values selected)" in rendered
    assert "- Hypothesis: short_reversal_21_0" in rendered
    assert "- Horizon: 63 trading sessions" in rendered
    assert "- Strategy net mean: 1.5000%" in rendered
    assert "- Equal-weight baseline net mean: 2.0000%" in rendered
    assert "- Difference vs baseline: -0.5000%" in rendered
    assert "- Walk-forward windows: 1/3 positive (0.333)" in rendered
    assert "- Label scramble: failed (p=0.1200, alpha=0.0500)" in rendered
    assert (
        "The registry rejected this hypothesis because the stored failure "
        "reason is `walk_forward_unstable`."
    ) in rendered
    assert "| 1 | 2024-04-01 to 2024-06-28 | 0.5000% | 2.5000% | -2.0000% |" in rendered
    assert "| AAPL | 40 | 3 | 3.2000% | 2/3 | 0.2500% |" in rendered
    assert "| MSFT | 30 | 2 | -1.0000% | 1/2 | -0.8000% |" in rendered


def test_loader_uses_read_only_sql_for_backtest_id() -> None:
    client = FakeJsonClient(_payload())

    explanation = load_hypothesis_evaluation_explanation(
        client,
        backtest_run_id=25,
    )

    assert explanation.identity.backtest_run_id == 25
    assert client.sql is not None
    assert client.sql.startswith("WITH target_backtest AS")
    upper_sql = client.sql.upper()
    assert "INSERT " not in upper_sql
    assert "UPDATE " not in upper_sql
    assert "DELETE " not in upper_sql


def test_loader_uses_latest_evaluation_for_hypothesis_key() -> None:
    client = FakeJsonClient(_payload())

    explanation = load_hypothesis_evaluation_explanation(
        client,
        hypothesis_key="short_reversal_21_0",
    )

    assert explanation.identity.hypothesis_key == "short_reversal_21_0"
    assert client.sql is not None
    assert "WHERE h.hypothesis_key = 'short_reversal_21_0'" in client.sql
    assert "ORDER BY he.created_at DESC, he.id DESC" in client.sql


def test_loader_requires_exactly_one_identity() -> None:
    with pytest.raises(HypothesisEvaluationExplanationError):
        load_hypothesis_evaluation_explanation(FakeJsonClient(_payload()))

    with pytest.raises(HypothesisEvaluationExplanationError):
        load_hypothesis_evaluation_explanation(
            FakeJsonClient(_payload()),
            backtest_run_id=25,
            hypothesis_key="short_reversal_21_0",
        )


def test_check_cli_validates_without_database_url() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--check", "--backtest-run-id", "25"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "OK: hypothesis evaluation explainer check passed" in result.stdout


def test_apply_cli_requires_database_url() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--backtest-run-id", "25"],
        text=True,
        capture_output=True,
        check=False,
        env={},
    )

    assert result.returncode == 1
    assert "DATABASE_URL is required unless --check is used" in result.stderr


def _payload() -> dict[str, Any]:
    return {
        "identity": {
            "hypothesis_key": "short_reversal_21_0",
            "hypothesis_name": "Short reversal 21-0",
            "hypothesis_status": "rejected",
            "hypothesis_thesis": "Recent losers may rebound.",
            "hypothesis_signal_name": "return_21_0",
            "hypothesis_mechanism": "Short-term reversal.",
            "evaluation_status": "rejected",
            "failure_reason": "walk_forward_unstable",
            "evaluation_notes": "feature candidate walk-forward v1 evaluation",
            "model_run_id": 25,
            "model_run_key": "model-key",
            "model_status": "succeeded",
            "model_code_git_sha": "abc123",
            "model_feature_set_hash": "feature-hash",
            "model_random_seed": 0,
            "model_training_start_date": "2024-01-02",
            "model_training_end_date": "2024-03-28",
            "model_test_start_date": "2024-04-01",
            "model_test_end_date": "2024-12-31",
            "model_available_at_policy_versions": {"daily_price": 1},
            "model_input_fingerprints": {
                "joined_feature_label_rows_sha256": "fingerprint"
            },
            "backtest_run_id": 25,
            "backtest_run_key": "backtest-key",
            "backtest_name": "return_21_0 falsifier backtest",
            "backtest_status": "succeeded",
            "universe_name": "falsifier_seed",
            "horizon_days": 63,
            "target_kind": "raw_return",
            "label_scramble_pass": False,
            "multiple_comparisons_correction": "none",
            "strategy": "return_21_0",
            "selection_direction": "low",
            "cost_assumptions": {"round_trip_cost_bps": 20.0},
        },
        "metrics": {
            "status": "succeeded",
            "failure_modes": [],
            "scored_test_dates": 189,
            "eligible_observations": 270,
            "selected_observations": 132,
            "mean_strategy_net_horizon_return": 0.015,
            "strategy_net_hit_rate": 0.58,
            "strategy_net_return_to_stddev": 0.21,
        },
        "baseline_metrics": {
            "equal_weight_universe": {
                "mean_net_horizon_return": 0.02,
            },
            "strategy_vs_equal_weight_universe": {
                "mean_net_difference": -0.005,
            },
        },
        "label_scramble_metrics": {
            "status": "completed",
            "p_value": 0.12,
            "alpha": 0.05,
            "seed": 44,
            "trial_count": 100,
            "observed_score": 0.015,
        },
        "metrics_by_regime": {
            "recent_seed_window_2024_2026": {
                "start_date": "2024-01-01",
                "end_date": "2026-12-31",
                "sample_count": 189,
                "strategy_net_return": {"mean": 0.015},
                "baseline_net_return": {"mean": 0.02},
                "net_difference_vs_baseline": {
                    "mean": -0.005,
                    "hit_rate": 0.3333333333333333,
                },
            }
        },
        "walk_forward_windows": [
            {
                "split_index": 0,
                "test_start": "2024-01-02",
                "test_end": "2024-03-28",
                "strategy_net_return": 0.03,
                "baseline_net_return": 0.02,
                "net_difference_vs_baseline": 0.01,
                "scored_dates": 63,
            },
            {
                "split_index": 1,
                "test_start": "2024-04-01",
                "test_end": "2024-06-28",
                "strategy_net_return": 0.005,
                "baseline_net_return": 0.025,
                "net_difference_vs_baseline": -0.02,
                "scored_dates": 63,
            },
            {
                "split_index": 2,
                "test_start": "2024-07-01",
                "test_end": "2024-09-30",
                "strategy_net_return": 0.01,
                "baseline_net_return": 0.015,
                "net_difference_vs_baseline": -0.005,
                "scored_dates": 63,
            },
        ],
        "ticker_attribution": [
            {
                "ticker": "AAPL",
                "selected_observations": 40,
                "selected_windows": 3,
                "positive_windows_selected": 2,
                "negative_windows_selected": 1,
                "mean_realized_return": 0.032,
                "mean_window_net_difference_when_selected": 0.0025,
            },
            {
                "ticker": "MSFT",
                "selected_observations": 30,
                "selected_windows": 2,
                "positive_windows_selected": 1,
                "negative_windows_selected": 1,
                "mean_realized_return": -0.01,
                "mean_window_net_difference_when_selected": -0.008,
            },
            {
                "ticker": "NVDA",
                "selected_observations": 20,
                "selected_windows": 1,
                "positive_windows_selected": 0,
                "negative_windows_selected": 1,
                "mean_realized_return": -0.02,
                "mean_window_net_difference_when_selected": -0.005,
            },
        ],
    }


class FakeJsonClient:
    def __init__(self, payload: dict[str, Any] | None) -> None:
        self.payload = payload
        self.sql: str | None = None

    def fetch_json(self, sql: str) -> Any:
        self.sql = sql
        return self.payload
