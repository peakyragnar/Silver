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


class FakeJsonClient:
    def __init__(self, payload: list[dict[str, Any]]) -> None:
        self.payload = payload
        self.sql: str | None = None

    def fetch_json(self, sql: str) -> Any:
        self.sql = sql
        return self.payload
