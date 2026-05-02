from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

from silver.features import feature_candidate_by_key
from silver.time.trading_calendar import CANONICAL_HORIZONS


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run_feature_candidate_horizon_sweep.py"


def load_horizon_sweep_cli():
    spec = importlib.util.spec_from_file_location(
        "run_feature_candidate_horizon_sweep",
        SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


horizon_sweep_cli = load_horizon_sweep_cli()


def test_check_mode_validates_horizon_sweep_without_database_url() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--check"],
        text=True,
        capture_output=True,
        check=False,
        env={},
    )

    assert result.returncode == 0
    assert "OK: feature candidate horizon sweep check passed" in result.stdout
    assert "55 candidate/horizon cell(s)" in result.stdout


def test_horizon_hypothesis_key_keeps_base_key_for_default_horizon() -> None:
    candidate = feature_candidate_by_key("momentum_6_1")

    assert horizon_sweep_cli.horizon_hypothesis_key(candidate, 63) == "momentum_6_1"
    assert (
        horizon_sweep_cli.horizon_hypothesis_key(candidate, 21)
        == "momentum_6_1__h21"
    )


def test_horizon_hypothesis_records_base_candidate_metadata() -> None:
    candidate = feature_candidate_by_key("short_reversal_21_0")

    hypothesis = horizon_sweep_cli.horizon_hypothesis(
        candidate,
        universe="falsifier_seed",
        horizon=5,
    )

    assert hypothesis.hypothesis_key == "short_reversal_21_0__h5"
    assert hypothesis.signal_name == "return_21_0"
    assert hypothesis.horizon_days == 5
    assert hypothesis.metadata == {
        "base_hypothesis_key": "short_reversal_21_0",
        "candidate_pack": "numeric_feature_pack_v1",
        "feature": "return_21_0",
        "horizon_sweep": True,
        "selection_direction": "low",
    }


def test_materialize_command_materializes_candidates_once() -> None:
    candidates = (
        feature_candidate_by_key("momentum_12_1"),
        feature_candidate_by_key("momentum_6_1"),
    )

    command = horizon_sweep_cli._materialize_command(
        candidates,
        universe="falsifier_seed",
        candidate_config_path=Path("/tmp/feature_candidates.yaml"),
    )

    assert "--candidate-config" in command
    assert "/tmp/feature_candidates.yaml" in command
    assert command.count("--candidate") == 2
    assert command[-4:] == [
        "--candidate",
        "momentum_12_1",
        "--candidate",
        "momentum_6_1",
    ]


def test_horizon_sweep_builds_low_direction_falsifier_command() -> None:
    candidate = feature_candidate_by_key("low_realized_volatility_63")

    command = horizon_sweep_cli._falsifier_command(
        candidate,
        universe="falsifier_seed",
        horizon=126,
        output_path=Path("reports/falsifier/horizon_sweep/low_vol_h126.md"),
    )

    assert "--strategy" in command
    assert "realized_volatility_63" in command
    assert "--horizon" in command
    assert "126" in command
    assert command[-2:] == ["--selection-direction", "low"]


def test_render_horizon_sweep_results_groups_by_candidate() -> None:
    rendered = horizon_sweep_cli.render_horizon_sweep_results(
        [
            horizon_sweep_cli.HorizonSweepResult(
                candidate_key="momentum_6_1",
                hypothesis_key="momentum_6_1",
                feature_name="momentum_6_1",
                selection_direction="high",
                horizon=63,
                action="skipped",
                backtest_run_id=42,
                walk_forward_status="failed",
                scored_windows=3,
                positive_windows=1,
                positive_window_rate=1 / 3,
                mean_net_difference_vs_baseline=-0.01,
                evaluation_status="rejected",
                failure_reason="walk_forward_unstable",
                report_path=Path("reports/falsifier/horizon_sweep/m_h63.md"),
            ),
            horizon_sweep_cli.HorizonSweepResult(
                candidate_key="momentum_6_1",
                hypothesis_key="momentum_6_1__h126",
                feature_name="momentum_6_1",
                selection_direction="high",
                horizon=126,
                action="evaluated",
                backtest_run_id=43,
                walk_forward_status="passed",
                scored_windows=4,
                positive_windows=3,
                positive_window_rate=0.75,
                mean_net_difference_vs_baseline=0.02,
                evaluation_status="promising",
                failure_reason=None,
                report_path=Path("reports/falsifier/horizon_sweep/m_h126.md"),
            ),
        ],
        horizons=CANONICAL_HORIZONS,
    )

    assert "candidate | feature | direction | 5 | 21 | 63 | 126 | 252" in rendered
    assert (
        "momentum_6_1 | momentum_6_1 | high | pending | pending | "
        "skipped:rejected | evaluated:promising | pending"
    ) in rendered
    assert "momentum_6_1 @ 63: 1/3 positive, mean_diff=-1.0000%" in rendered
