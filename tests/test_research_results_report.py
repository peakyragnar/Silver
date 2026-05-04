from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

from silver.analytics.research_results import (
    load_research_results_report,
    render_research_results_report,
)


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "research_results_report.py"


def test_render_research_results_report_summarizes_candidate_verdicts() -> None:
    report = load_research_results_report(FakeJsonClient(_payload()))

    rendered = render_research_results_report(report)

    assert "Research Results v0" in rendered
    assert "- Hypotheses: 4" in rendered
    assert "- Tested hypotheses: 3" in rendered
    assert "- Untested hypotheses: 1" in rendered
    assert "| price | 2 | 0 | 0 | 1 | 1 | 0 |" in rendered
    assert "| fundamentals | 2 | 0 | 0 | 1 | 0 | 1 |" in rendered
    assert (
        "| momentum_12_1 | price | momentum_12_1 | 63 | high | rejected | "
        "4.4600% | 4.5000% | -0.0400% | baseline_failed | 101 |"
    ) in rendered
    assert (
        "| revenue_growth_yoy | fundamentals | revenue_growth_yoy | 63 | high | "
        "rejected | 4.5100% | 4.4900% | 0.0200% | label_scramble_failed | 102 |"
    ) in rendered
    assert (
        "| low_realized_volatility_63 | price | realized_volatility_63 | 63 | "
        "low | insufficient_data | n/a | n/a | n/a | insufficient_data | 103 |"
    ) in rendered
    assert (
        "| gross_margin | fundamentals | gross_margin | n/a | high | untested | "
        "n/a | n/a | n/a | not_run | n/a |"
    ) in rendered
    assert (
        "Try canonical neighboring horizons for `revenue_growth_yoy`; it beat "
        "baseline but failed label-scramble at 63 sessions."
    ) in rendered
    assert (
        "Materialize or run the falsifier for untested hypotheses: gross_margin."
    ) in rendered
    assert "Reason Glossary:" in rendered
    assert (
        "| baseline_failed | The strategy did not beat the equal-weight universe "
        "baseline after costs for the tested horizon. |"
    ) in rendered
    assert (
        "| label_scramble_failed | Randomly reassigned labels produced results "
        "too close to the observed result, so the apparent signal is not robust "
        "evidence. |"
    ) in rendered
    assert (
        "| insufficient_data | The run ended as a terminal no-claim result "
        "because too few usable feature, label, or walk-forward rows were "
        "available. |"
    ) in rendered
    assert (
        "| not_run | The hypothesis exists in the registry but no linked "
        "falsifier evaluation is available yet. |"
    ) in rendered
    assert "Promising Candidate Summary:" in rendered
    assert "No promising cells found." in rendered
    assert "Promising Candidate Review:" in rendered
    assert "No promising cells found." in rendered
    assert "Promising Deep Dive v0:" in rendered
    assert (
        "No stored row found for `avg_dollar_volume_63__h252`. Run the horizon "
        "sweep before opening the first deep dive."
    ) in rendered


def test_loader_uses_read_only_registry_sql() -> None:
    client = FakeJsonClient(_payload())

    report = load_research_results_report(client)

    assert report.results
    assert client.sql is not None
    assert client.sql.startswith("WITH latest_evaluations AS")
    upper_sql = client.sql.upper()
    assert "INSERT " not in upper_sql
    assert "UPDATE " not in upper_sql
    assert "DELETE " not in upper_sql


def test_render_groups_horizon_sweep_rows_into_matrix_and_heatmap() -> None:
    payload = [
        *_payload(),
        {
            "hypothesis_key": "momentum_12_1__h21",
            "hypothesis_name": "Momentum 12-1 (21d)",
            "hypothesis_status": "rejected",
            "hypothesis_signal_name": "momentum_12_1",
            "hypothesis_universe_name": "falsifier_seed",
            "hypothesis_horizon_days": 21,
            "hypothesis_target_kind": "raw_return",
            "hypothesis_metadata": {
                "base_hypothesis_key": "momentum_12_1",
                "horizon_sweep": True,
                "selection_direction": "high",
            },
            "evaluation_status": "rejected",
            "failure_reason": "walk_forward_unstable",
            "backtest_run_id": 104,
            "backtest_run_key": "backtest-momentum-h21",
            "backtest_status": "succeeded",
            "backtest_universe_name": "falsifier_seed",
            "backtest_horizon_days": 21,
            "backtest_target_kind": "raw_return",
            "backtest_parameters": {"strategy": "momentum_12_1"},
            "backtest_metrics": {
                "mean_strategy_net_horizon_return": 0.012,
                "scored_test_dates": 126,
                "walk_forward_windows": [
                    {
                        "test_start": "2015-01-02",
                        "test_end": "2015-01-30",
                        "net_difference_vs_baseline": 0.01,
                    },
                    {
                        "test_start": "2015-02-02",
                        "test_end": "2015-02-27",
                        "net_difference_vs_baseline": -0.02,
                    },
                ],
            },
            "baseline_metrics": {
                "equal_weight_universe": {
                    "mean_net_horizon_return": 0.011,
                },
                "strategy_vs_equal_weight_universe": {
                    "mean_net_difference": 0.001,
                },
            },
            "label_scramble_metrics": {
                "status": "completed",
                "p_value": 0.01,
                "alpha": 0.05,
            },
            "label_scramble_pass": True,
            "model_run_id": 204,
            "model_run_key": "model-momentum-h21",
            "model_status": "succeeded",
            "model_parameters": {"strategy": "momentum_12_1"},
        },
    ]

    report = load_research_results_report(FakeJsonClient(payload))
    rendered = render_research_results_report(report)

    assert "Horizon Matrix:" in rendered
    assert (
        "| momentum_12_1 | price | momentum_12_1 | pending | "
        "rejected:walk_forward_unstable | rejected:baseline_failed | "
        "pending | pending |"
    ) in rendered
    assert "Bucket Heatmaps:" in rendered
    assert (
        "| momentum_12_1 | 21 | 1/2 | 2015:+- | "
        "`+` beat baseline; `-` failed baseline. |"
    ) in rendered


def test_render_promising_candidate_review_recommends_next_actions() -> None:
    payload = [
        *_payload(),
        {
            "hypothesis_key": "avg_dollar_volume_63__h126",
            "hypothesis_name": "Average Dollar Volume 63 (126d)",
            "hypothesis_status": "rejected",
            "hypothesis_signal_name": "avg_dollar_volume_63",
            "hypothesis_universe_name": "falsifier_seed",
            "hypothesis_horizon_days": 126,
            "hypothesis_target_kind": "raw_return",
            "hypothesis_metadata": {
                "base_hypothesis_key": "avg_dollar_volume_63",
                "horizon_sweep": True,
                "selection_direction": "high",
            },
            "evaluation_status": "rejected",
            "failure_reason": "walk_forward_unstable",
            "backtest_run_id": 104,
            "backtest_run_key": "backtest-dollar-volume-h126",
            "backtest_status": "succeeded",
            "backtest_universe_name": "falsifier_seed",
            "backtest_horizon_days": 126,
            "backtest_target_kind": "raw_return",
            "backtest_parameters": {"strategy": "avg_dollar_volume_63"},
            "backtest_metrics": {
                "mean_strategy_net_horizon_return": 0.1000,
                "walk_forward_windows": [
                    {
                        "test_start": "2020-01-02",
                        "test_end": "2020-04-01",
                        "net_difference_vs_baseline": 0.005,
                    }
                ],
            },
            "baseline_metrics": {
                "equal_weight_universe": {
                    "mean_net_horizon_return": 0.0950,
                },
                "strategy_vs_equal_weight_universe": {
                    "mean_net_difference": 0.005,
                },
            },
            "backtest_cost_assumptions": {"round_trip_cost_bps": 10},
            "label_scramble_metrics": {
                "status": "completed",
                "p_value": 0.012,
                "alpha": 0.05,
            },
            "label_scramble_pass": True,
            "model_run_id": 204,
            "model_run_key": "model-dollar-volume-h126",
            "model_status": "succeeded",
            "model_parameters": {"strategy": "avg_dollar_volume_63"},
        },
        {
            "hypothesis_key": "avg_dollar_volume_63__h252",
            "hypothesis_name": "Average Dollar Volume 63 (252d)",
            "hypothesis_status": "promising",
            "hypothesis_signal_name": "avg_dollar_volume_63",
            "hypothesis_universe_name": "falsifier_seed",
            "hypothesis_horizon_days": 252,
            "hypothesis_target_kind": "raw_return",
            "hypothesis_metadata": {
                "base_hypothesis_key": "avg_dollar_volume_63",
                "horizon_sweep": True,
                "selection_direction": "high",
            },
            "evaluation_status": "promising",
            "failure_reason": None,
            "backtest_run_id": 105,
            "backtest_run_key": "backtest-dollar-volume-h252",
            "backtest_status": "succeeded",
            "backtest_universe_name": "falsifier_seed",
            "backtest_horizon_days": 252,
            "backtest_target_kind": "raw_return",
            "backtest_parameters": {"strategy": "avg_dollar_volume_63"},
            "backtest_metrics": {
                "mean_strategy_net_horizon_return": 0.2100,
                "walk_forward_windows": [
                    {
                        "test_start": "2020-01-02",
                        "test_end": "2020-12-31",
                        "net_difference_vs_baseline": 0.020,
                    },
                    {
                        "test_start": "2021-01-04",
                        "test_end": "2021-12-31",
                        "net_difference_vs_baseline": -0.002,
                    },
                    {
                        "test_start": "2022-01-03",
                        "test_end": "2022-12-30",
                        "net_difference_vs_baseline": 0.030,
                    },
                ],
            },
            "baseline_metrics": {
                "equal_weight_universe": {
                    "mean_net_horizon_return": 0.1940,
                },
                "strategy_vs_equal_weight_universe": {
                    "mean_net_difference": 0.016,
                },
            },
            "backtest_cost_assumptions": {"round_trip_cost_bps": 10},
            "label_scramble_metrics": {
                "status": "completed",
                "p_value": 0.010,
                "alpha": 0.05,
            },
            "label_scramble_pass": True,
            "model_run_id": 205,
            "model_run_key": "model-dollar-volume-h252",
            "model_status": "succeeded",
            "model_parameters": {"strategy": "avg_dollar_volume_63"},
        },
    ]

    report = load_research_results_report(
        FakeJsonClient(
            payload,
            _selection_explanation_payload(
                [
                    {
                        "ticker": "AAPL",
                        "selected_observations": 2,
                        "selected_windows": 2,
                        "positive_windows_selected": 2,
                        "negative_windows_selected": 0,
                        "mean_realized_return": 0.05,
                        "mean_window_net_difference_when_selected": 0.01,
                    },
                    {
                        "ticker": "MSFT",
                        "selected_observations": 2,
                        "selected_windows": 2,
                        "positive_windows_selected": 1,
                        "negative_windows_selected": 1,
                        "mean_realized_return": 0.02,
                        "mean_window_net_difference_when_selected": 0.005,
                    },
                    {
                        "ticker": "NVDA",
                        "selected_observations": 1,
                        "selected_windows": 1,
                        "positive_windows_selected": 0,
                        "negative_windows_selected": 1,
                        "mean_realized_return": -0.01,
                        "mean_window_net_difference_when_selected": -0.002,
                    },
                ]
            ),
        )
    )
    rendered = render_research_results_report(report)

    assert "Promising Candidate Summary:" in rendered
    assert (
        "1. avg_dollar_volume_63__h252\n"
        "   recommendation: deep_dive\n"
        "   horizon: 252 trading sessions\n"
        "   edge: +1.6000%\n"
        "   buckets: 2/3 (66.7%)\n"
        "   label scramble: pass p=0.0100 <= 0.0500\n"
        "   cost sensitivity: low (16.0x current cost)\n"
        "   reason: large edge with usable cost cushion; inspect drivers and "
        "replay evidence"
    ) in rendered
    assert "Promising Candidate Review:" in rendered
    assert (
        "| avg_dollar_volume_63__h252 | 252 | 2/3 (66.7%) | 1.6000% | "
        "h126 rejected:walk_forward_unstable | pass p=0.0100 <= 0.0500 | "
        "low (16.0x current cost) | deep_dive | large edge with usable "
        "cost cushion; inspect drivers and replay evidence |"
    ) in rendered
    assert "Promising Deep Dive v0:" in rendered
    assert "Cell: avg_dollar_volume_63__h252" in rendered
    assert "Recommendation: watch" in rendered
    assert "Reason: adjacent horizon evidence is mixed" in rendered
    assert (
        "- temporal concentration: largest positive bucket contributes 60.0% "
        "of positive bucket edge (2022-01-03 to 2022-12-30)"
    ) in rendered
    assert (
        "- ticker concentration: top ticker AAPL is 2/5 selections (40.0%); "
        "top 5 are 5/5 (100.0%); HHI 0.360, effective tickers 2.8"
    ) in rendered
    assert (
        "| 2021 | 0/1 (0.0%) | -0.2000% | - |"
    ) in rendered
    assert (
        "| h126 | rejected:walk_forward_unstable | +0.5000% | 1/1 (100.0%) | "
        "low (5.0x current cost) |"
    ) in rendered
    assert (
        "momentum_12_1:\n"
        "- h63: rejected:baseline_failed, edge -0.0400%, buckets 0/1 (0.0%)\n"
        "- h126: pending\n"
        "- h252: pending"
    ) in rendered
    assert (
        "- source: reconstructed read-only from persisted feature values, "
        "forward-return labels, and walk-forward windows."
    ) in rendered
    assert "- momentum_12_1__h252: attribution unavailable" in rendered
    assert (
        "| AAPL | 2 | 40.0% | 2 | +5.0000% | 2/2 | +1.0000% |"
    ) in rendered
    assert "selected attribution is not available" not in rendered


def test_check_cli_validates_without_database_url() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--check"],
        text=True,
        capture_output=True,
        check=False,
        env={},
    )

    assert result.returncode == 0
    assert "OK: research results report check passed" in result.stdout


def test_apply_cli_requires_database_url() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT)],
        text=True,
        capture_output=True,
        check=False,
        env={},
    )

    assert result.returncode == 1
    assert "DATABASE_URL is required unless --check is used" in result.stderr


def _payload() -> list[dict[str, Any]]:
    return [
        {
            "hypothesis_key": "momentum_12_1",
            "hypothesis_name": "Momentum 12-1",
            "hypothesis_status": "rejected",
            "hypothesis_signal_name": "momentum_12_1",
            "hypothesis_universe_name": "falsifier_seed",
            "hypothesis_horizon_days": 63,
            "hypothesis_target_kind": "raw_return",
            "hypothesis_metadata": {},
            "evaluation_status": "rejected",
            "failure_reason": None,
            "backtest_run_id": 101,
            "backtest_run_key": "backtest-momentum",
            "backtest_status": "succeeded",
            "backtest_universe_name": "falsifier_seed",
            "backtest_horizon_days": 63,
            "backtest_target_kind": "raw_return",
            "backtest_parameters": {"strategy": "momentum_12_1"},
            "backtest_metrics": {
                "mean_strategy_net_horizon_return": 0.0446,
                "scored_test_dates": 189,
                "walk_forward_windows": [
                    {
                        "test_start": "2016-01-04",
                        "test_end": "2016-04-04",
                        "net_difference_vs_baseline": -0.0004,
                    }
                ],
            },
            "baseline_metrics": {
                "equal_weight_universe": {
                    "mean_net_horizon_return": 0.0450,
                },
                "strategy_vs_equal_weight_universe": {
                    "mean_net_difference": -0.0004,
                },
            },
            "label_scramble_metrics": {
                "status": "completed",
                "p_value": 0.01,
                "alpha": 0.05,
            },
            "label_scramble_pass": True,
            "model_run_id": 201,
            "model_run_key": "model-momentum",
            "model_status": "succeeded",
            "model_parameters": {"strategy": "momentum_12_1"},
        },
        {
            "hypothesis_key": "revenue_growth_yoy",
            "hypothesis_name": "Revenue Growth YoY",
            "hypothesis_status": "rejected",
            "hypothesis_signal_name": "revenue_growth_yoy",
            "hypothesis_universe_name": "falsifier_seed",
            "hypothesis_horizon_days": 63,
            "hypothesis_target_kind": "raw_return",
            "hypothesis_metadata": {},
            "evaluation_status": "rejected",
            "failure_reason": None,
            "backtest_run_id": 102,
            "backtest_run_key": "backtest-revenue-growth",
            "backtest_status": "succeeded",
            "backtest_universe_name": "falsifier_seed",
            "backtest_horizon_days": 63,
            "backtest_target_kind": "raw_return",
            "backtest_parameters": {"strategy": "revenue_growth_yoy"},
            "backtest_metrics": {
                "mean_strategy_net_horizon_return": 0.0451,
                "scored_test_dates": 189,
            },
            "baseline_metrics": {
                "equal_weight_universe": {
                    "mean_net_horizon_return": 0.0449,
                },
                "strategy_vs_equal_weight_universe": {
                    "mean_net_difference": 0.0002,
                },
            },
            "label_scramble_metrics": {
                "status": "completed",
                "p_value": 0.18,
                "alpha": 0.05,
            },
            "label_scramble_pass": False,
            "model_run_id": 202,
            "model_run_key": "model-revenue-growth",
            "model_status": "succeeded",
            "model_parameters": {"strategy": "revenue_growth_yoy"},
        },
        {
            "hypothesis_key": "low_realized_volatility_63",
            "hypothesis_name": "Low Realized Volatility 63",
            "hypothesis_status": "rejected",
            "hypothesis_signal_name": "realized_volatility_63",
            "hypothesis_universe_name": "falsifier_seed",
            "hypothesis_horizon_days": 63,
            "hypothesis_target_kind": "raw_return",
            "hypothesis_metadata": {},
            "evaluation_status": "rejected",
            "failure_reason": "insufficient_data",
            "backtest_run_id": 103,
            "backtest_run_key": "backtest-low-vol",
            "backtest_status": "insufficient_data",
            "backtest_universe_name": "falsifier_seed",
            "backtest_horizon_days": 63,
            "backtest_target_kind": "raw_return",
            "backtest_parameters": {
                "strategy": "realized_volatility_63",
                "selection_direction": "low",
            },
            "backtest_metrics": {"status": "insufficient_data"},
            "baseline_metrics": {},
            "label_scramble_metrics": {"status": "not_run"},
            "label_scramble_pass": False,
            "model_run_id": 203,
            "model_run_key": "model-low-vol",
            "model_status": "insufficient_data",
            "model_parameters": {
                "strategy": "realized_volatility_63",
                "selection_direction": "low",
            },
        },
        {
            "hypothesis_key": "gross_margin",
            "hypothesis_name": "Gross Margin",
            "hypothesis_status": "proposed",
            "hypothesis_signal_name": "gross_margin",
            "hypothesis_universe_name": "falsifier_seed",
            "hypothesis_horizon_days": None,
            "hypothesis_target_kind": None,
            "hypothesis_metadata": {},
            "evaluation_status": None,
            "failure_reason": None,
            "backtest_run_id": None,
            "backtest_run_key": None,
            "backtest_status": None,
            "backtest_universe_name": None,
            "backtest_horizon_days": None,
            "backtest_target_kind": None,
            "backtest_parameters": None,
            "backtest_metrics": None,
            "baseline_metrics": None,
            "label_scramble_metrics": None,
            "label_scramble_pass": None,
            "model_run_id": None,
            "model_run_key": None,
            "model_status": None,
            "model_parameters": None,
        },
    ]


def _selection_explanation_payload(
    ticker_attribution: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "identity": {
            "model_run_id": 205,
            "model_run_key": "model-dollar-volume-h252",
            "model_status": "succeeded",
            "model_code_git_sha": "abc123",
            "model_feature_set_hash": "feature-hash",
            "model_random_seed": 0,
            "model_training_start_date": "2020-01-02",
            "model_training_end_date": "2020-12-31",
            "model_test_start_date": "2021-01-04",
            "model_test_end_date": "2022-12-30",
            "model_available_at_policy_versions": {"daily_price": 1},
            "model_input_fingerprints": {
                "joined_feature_label_rows_sha256": "fingerprint"
            },
            "backtest_run_id": 105,
            "backtest_run_key": "backtest-dollar-volume-h252",
            "backtest_name": "Average Dollar Volume 63 (252d)",
            "backtest_status": "succeeded",
            "universe_name": "falsifier_seed",
            "horizon_days": 252,
            "target_kind": "raw_return",
            "label_scramble_pass": True,
            "multiple_comparisons_correction": "none",
            "strategy": "avg_dollar_volume_63",
            "selection_direction": "high",
            "cost_assumptions": {"round_trip_cost_bps": 10},
            "hypothesis_key": "avg_dollar_volume_63__h252",
            "hypothesis_name": "Average Dollar Volume 63 (252d)",
            "hypothesis_status": "promising",
            "hypothesis_thesis": "Liquidity may proxy future returns.",
            "hypothesis_signal_name": "avg_dollar_volume_63",
            "hypothesis_mechanism": "Liquidity exposure.",
            "evaluation_status": "promising",
            "failure_reason": None,
            "evaluation_notes": "feature candidate walk-forward v1 evaluation",
        },
        "metrics": {},
        "baseline_metrics": {},
        "label_scramble_metrics": {},
        "metrics_by_regime": {},
        "walk_forward_windows": [],
        "ticker_attribution": ticker_attribution,
    }


class FakeJsonClient:
    def __init__(self, payload: Any, *extra_payloads: Any) -> None:
        self.payloads = [payload, *extra_payloads]
        self.sql: str | None = None
        self.sqls: list[str] = []

    def fetch_json(self, sql: str) -> Any:
        self.sql = sql
        self.sqls.append(sql)
        if len(self.payloads) == 1:
            return self.payloads[0]
        return self.payloads.pop(0)
