from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

from silver.features import feature_candidate_by_key


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run_feature_candidate_walk_forward.py"


def load_candidate_walk_forward_cli():
    spec = importlib.util.spec_from_file_location(
        "run_feature_candidate_walk_forward",
        SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


candidate_walk_forward_cli = load_candidate_walk_forward_cli()


def test_check_mode_validates_candidate_walk_forward_without_database_url() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--check"],
        text=True,
        capture_output=True,
        check=False,
        env={},
    )

    assert result.returncode == 0
    assert "OK: feature candidate walk-forward check passed" in result.stdout
    assert "momentum_6_1" in result.stdout


def test_walk_forward_evidence_passes_when_enough_windows_beat_baseline() -> None:
    evidence = candidate_walk_forward_cli.walk_forward_evidence_from_metrics(
        {
            "walk_forward_windows": [
                {"net_difference_vs_baseline": 0.01},
                {"net_difference_vs_baseline": 0.02},
                {"net_difference_vs_baseline": -0.01},
            ]
        },
        min_scored_windows=2,
        min_positive_window_rate=0.6,
    )

    assert evidence.scored_windows == 3
    assert evidence.positive_windows == 2
    assert evidence.positive_window_rate == 2 / 3
    assert evidence.mean_net_difference_vs_baseline == pytest.approx(0.02 / 3)
    assert evidence.walk_forward_status == "passed"
    assert evidence.failure_reason is None


def test_walk_forward_evidence_rejects_unstable_windows() -> None:
    evidence = candidate_walk_forward_cli.walk_forward_evidence_from_metrics(
        {
            "walk_forward_windows": [
                {"strategy_net_return": 0.01, "baseline_net_return": 0.02},
                {"strategy_net_return": 0.03, "baseline_net_return": 0.01},
                {"strategy_net_return": 0.00, "baseline_net_return": 0.01},
            ]
        },
        min_scored_windows=2,
        min_positive_window_rate=0.6,
    )

    assert evidence.walk_forward_status == "failed"
    assert evidence.failure_reason == "walk_forward_unstable"


def test_walk_forward_evidence_requires_minimum_windows() -> None:
    evidence = candidate_walk_forward_cli.walk_forward_evidence_from_metrics(
        {"walk_forward_windows": [{"net_difference_vs_baseline": 0.10}]},
        min_scored_windows=2,
        min_positive_window_rate=0.5,
    )

    assert evidence.walk_forward_status == "insufficient_data"
    assert evidence.failure_reason == "insufficient_walk_forward_windows"


def test_harder_status_rejects_failed_walk_forward_before_registry_inference() -> None:
    evidence = candidate_walk_forward_cli.WalkForwardEvidence(
        scored_windows=3,
        positive_windows=1,
        positive_window_rate=1 / 3,
        mean_net_difference_vs_baseline=-0.01,
        walk_forward_status="failed",
        failure_reason="walk_forward_unstable",
    )

    status, failure = candidate_walk_forward_cli.harder_evaluation_status(
        evidence,
        label_scramble_pass=True,
    )

    assert status == "rejected"
    assert failure == "walk_forward_unstable"


def test_harder_status_rejects_failed_label_scramble() -> None:
    evidence = candidate_walk_forward_cli.WalkForwardEvidence(
        scored_windows=3,
        positive_windows=3,
        positive_window_rate=1.0,
        mean_net_difference_vs_baseline=0.01,
        walk_forward_status="passed",
        failure_reason=None,
    )

    status, failure = candidate_walk_forward_cli.harder_evaluation_status(
        evidence,
        label_scramble_pass=False,
    )

    assert status == "rejected"
    assert failure == "label_scramble_failed"


def test_candidate_walk_forward_builds_low_direction_falsifier_command() -> None:
    candidate = feature_candidate_by_key("short_reversal_21_0")

    command = candidate_walk_forward_cli._falsifier_command(
        candidate,
        universe="falsifier_seed",
        horizon=63,
        output_path=Path("reports/falsifier/candidate_walk_forward/reversal.md"),
    )

    assert "--strategy" in command
    assert "return_21_0" in command
    assert "--database-url" not in command
    assert command[-2:] == ["--selection-direction", "low"]


def test_render_walk_forward_results_includes_consistency_rollup() -> None:
    rendered = candidate_walk_forward_cli.render_walk_forward_results(
        [
            candidate_walk_forward_cli.CandidateWalkForwardResult(
                candidate_key="momentum_6_1",
                feature_name="momentum_6_1",
                selection_direction="high",
                backtest_run_id=42,
                walk_forward_status="failed",
                scored_windows=3,
                positive_windows=1,
                positive_window_rate=1 / 3,
                mean_net_difference_vs_baseline=-0.01,
                evaluation_status="rejected",
                failure_reason="walk_forward_unstable",
                report_path=Path("reports/falsifier/candidate_walk_forward/m.md"),
            )
        ]
    )

    assert "momentum_6_1 | momentum_6_1 | high | failed | 1/3" in rendered
    assert "rejected (walk_forward_unstable)" in rendered
