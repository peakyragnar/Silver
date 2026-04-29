from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from silver.backtest.momentum_falsifier import (
    MomentumBacktestRow,
    run_momentum_falsifier,
)
from silver.reports.falsifier import (
    FalsifierFeatureMetadata,
    FalsifierInputCounts,
    FalsifierModelWindow,
    FalsifierReport,
    FalsifierReproducibilityMetadata,
    FalsifierRunIdentity,
    UniverseMember,
    coverage_from_rows,
    fingerprint_momentum_inputs,
    missing_prerequisite_message,
    render_week_1_momentum_report,
)
from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


def test_report_rendering_is_deterministic_and_contains_required_sections() -> None:
    calendar, sessions = _synthetic_calendar(16)
    rows = (
        _row("AAA", sessions[0], sessions[7], 0.10, 0.01),
        _row("BBB", sessions[0], sessions[7], 0.20, 0.03),
        _row("AAA", sessions[1], sessions[8], 0.40, -0.01),
        _row("BBB", sessions[1], sessions[8], 0.30, 0.02),
        _row("AAA", sessions[7], sessions[12], 0.50, 0.04),
        _row("BBB", sessions[7], sessions[12], 0.10, 0.01),
        _row("AAA", sessions[8], sessions[13], 0.20, 0.00),
        _row("BBB", sessions[8], sessions[13], 0.80, 0.05),
    )
    result = run_momentum_falsifier(
        rows,
        calendar=calendar,
        horizon_sessions=5,
        min_train_sessions=2,
        test_sessions=2,
        step_sessions=2,
        round_trip_cost_bps=20.0,
    )
    assert result.status == "succeeded"

    report = FalsifierReport(
        strategy="momentum_12_1",
        horizon=5,
        universe_name="falsifier_seed",
        universe_members=(
            UniverseMember("AAA", date(2024, 1, 1), None),
            UniverseMember("BBB", date(2024, 1, 1), None),
        ),
        data_coverage=coverage_from_rows(rows),
        feature_metadata=FalsifierFeatureMetadata(
            name="momentum_12_1",
            version=1,
            definition_hash="a" * 64,
            feature_set_hash="a" * 64,
        ),
        backtest_result=result,
        reproducibility=FalsifierReproducibilityMetadata(
            command=(
                "python scripts/run_falsifier.py --strategy momentum_12_1 "
                "--horizon 5 --universe falsifier_seed"
            ),
            git_sha="f" * 40,
            input_fingerprint=fingerprint_momentum_inputs(rows),
            available_at_policy_versions={"daily_price": 1},
            run_identity=FalsifierRunIdentity(
                model_run_id=101,
                model_run_key="model-run-momentum-12-1-202401",
                backtest_run_id=202,
                backtest_run_key="backtest-run-momentum-12-1-202401",
            ),
            model_window=FalsifierModelWindow(
                training_start_date=sessions[0],
                training_end_date=sessions[7],
                test_start_date=sessions[8],
                test_end_date=sessions[13],
                source="scorable_walk_forward",
            ),
            target_kind="excess_return_market",
            random_seed=0,
            execution_assumptions={
                "label_scramble_alpha": 0.05,
                "label_scramble_seed": 44,
                "label_scramble_trial_count": 100,
                "min_train_sessions": 2,
                "multiple_comparisons_correction": "none",
                "round_trip_cost_bps": 20.0,
                "step_sessions": 2,
                "test_sessions": 2,
            },
        ),
    )

    rendered = render_week_1_momentum_report(report)

    assert rendered == render_week_1_momentum_report(report)
    assert "# Week 1 Momentum Falsifier Report" in rendered
    assert "No alpha claim is made" in rendered
    assert "## Data Coverage" in rendered
    assert "## Universe" in rendered
    assert "## Train/Test Windows" in rendered
    assert "## Headline Metrics" in rendered
    assert "## Baseline Comparison" in rendered
    assert "## Costs Assumption" in rendered
    assert "## Failure Modes" in rendered
    assert "## Reproducibility" in rendered
    assert "| Horizon | 5 trading sessions |" in rendered
    assert "| Feature version | momentum_12_1 v1 |" in rendered
    assert "| model_run_id | 101 |" in rendered
    assert "| model_run_key | model-run-momentum-12-1-202401 |" in rendered
    assert "| backtest_run_id | 202 |" in rendered
    assert "| backtest_run_key | backtest-run-momentum-12-1-202401 |" in rendered
    assert "| Git SHA | " + "f" * 40 + " |" in rendered
    assert "| Feature definition hash | " + "a" * 64 + " |" in rendered
    assert "| Feature set hash | " + "a" * 64 + " |" in rendered
    assert "| Model training window | 2024-01-02 to 2024-01-09 |" in rendered
    assert "| Model test window | 2024-01-10 to 2024-01-15 |" in rendered
    assert "| Model window source | scorable_walk_forward |" in rendered
    assert "| Target kind | excess_return_market |" in rendered
    assert "| Input fingerprint | " + fingerprint_momentum_inputs(rows) + " |" in rendered
    assert "| Available-at policy versions | `{\"daily_price\":1}` |" in rendered
    assert "| Random seed | 0 |" in rendered
    assert (
        "| Execution assumptions | "
        "`{\"label_scramble_alpha\":0.05,\"label_scramble_seed\":44,"
        "\"label_scramble_trial_count\":100,\"min_train_sessions\":2,"
        "\"multiple_comparisons_correction\":\"none\","
        "\"round_trip_cost_bps\":20.0,\"step_sessions\":2,\"test_sessions\":2}`"
        " |"
    ) in rendered
    assert "| Report schema version | 3 |" in rendered


def test_missing_prerequisite_message_names_materialization_step() -> None:
    message = missing_prerequisite_message(
        FalsifierInputCounts(
            universe_members=5,
            feature_values=0,
            labels=25,
            joined_rows=0,
        ),
        strategy="momentum_12_1",
        horizon=63,
        universe="falsifier_seed",
    )

    assert message is not None
    assert "momentum feature materialization step" in message
    assert "momentum_12_1" in message


def test_insufficient_data_is_valid_report_status() -> None:
    calendar, sessions = _synthetic_calendar(4)
    rows = (_row("AAA", sessions[0], sessions[1], 0.10, 0.01),)

    result = run_momentum_falsifier(
        rows,
        calendar=calendar,
        horizon_sessions=5,
        min_train_sessions=2,
        test_sessions=1,
        step_sessions=1,
    )

    assert result.status == "insufficient_data"
    assert result.failure_modes


def _row(
    ticker: str,
    asof_date: date,
    horizon_date: date,
    feature_value: float,
    realized_return: float,
) -> MomentumBacktestRow:
    return MomentumBacktestRow(
        ticker=ticker,
        asof_date=asof_date,
        horizon_date=horizon_date,
        feature_value=feature_value,
        realized_return=realized_return,
    )


def _synthetic_calendar(session_count: int) -> tuple[TradingCalendar, list[date]]:
    start = date(2024, 1, 2)
    rows: list[TradingCalendarRow] = []
    sessions: list[date] = []
    current = start
    while len(sessions) < session_count:
        rows.append(
            TradingCalendarRow(
                date=current,
                is_session=True,
                session_close=datetime.combine(current, datetime.min.time(), timezone.utc)
                + timedelta(hours=21),
            )
        )
        sessions.append(current)
        current += timedelta(days=1)
    return TradingCalendar(rows), sessions
