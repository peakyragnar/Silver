"""Thin Phase 1 momentum falsifier over persisted feature and label rows."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal, Sequence

from silver.backtest.walk_forward import WalkForwardConfig, plan_walk_forward_splits
from silver.time.trading_calendar import CANONICAL_HORIZONS, TradingCalendar


DEFAULT_MIN_TRAIN_SESSIONS = 252
DEFAULT_TEST_SESSIONS = 63
DEFAULT_STEP_SESSIONS = 63
DEFAULT_ROUND_TRIP_COST_BPS = 20.0

MomentumFalsifierStatus = Literal["succeeded", "insufficient_data"]


class MomentumFalsifierInputError(ValueError):
    """Raised when persisted falsifier inputs are malformed."""


@dataclass(frozen=True, slots=True)
class MomentumBacktestRow:
    """One persisted feature/label pair available to the falsifier."""

    ticker: str
    asof_date: date
    horizon_date: date
    feature_value: float
    realized_return: float


@dataclass(frozen=True, slots=True)
class MomentumDateResult:
    """One scored test date in a walk-forward window."""

    asof_date: date
    eligible_count: int
    selected_tickers: tuple[str, ...]
    strategy_gross_return: float
    strategy_net_return: float
    baseline_gross_return: float
    baseline_net_return: float


@dataclass(frozen=True, slots=True)
class MomentumWindowResult:
    """Aggregated scores for one scorable walk-forward split."""

    split_index: int
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    train_observations: int
    test_observations: int
    scored_dates: int
    selected_observations: int
    strategy_net_return: float | None
    baseline_net_return: float | None
    date_results: tuple[MomentumDateResult, ...]


@dataclass(frozen=True, slots=True)
class MomentumHeadlineMetrics:
    """Headline metrics for the thin Phase 1 falsifier."""

    split_count: int
    scored_test_dates: int
    eligible_observations: int
    selected_observations: int
    mean_strategy_gross_return: float | None
    mean_strategy_net_return: float | None
    mean_baseline_gross_return: float | None
    mean_baseline_net_return: float | None
    mean_net_difference_vs_baseline: float | None
    strategy_net_hit_rate: float | None
    strategy_net_return_stddev: float | None
    strategy_net_return_to_stddev: float | None


@dataclass(frozen=True, slots=True)
class MomentumFalsifierResult:
    """Complete deterministic output of the thin momentum falsifier."""

    status: MomentumFalsifierStatus
    horizon_sessions: int
    round_trip_cost_bps: float
    min_train_sessions: int
    test_sessions: int
    step_sessions: int
    windows: tuple[MomentumWindowResult, ...]
    headline_metrics: MomentumHeadlineMetrics
    failure_modes: tuple[str, ...]


def run_momentum_falsifier(
    rows: Sequence[MomentumBacktestRow],
    *,
    calendar: TradingCalendar,
    horizon_sessions: int,
    min_train_sessions: int = DEFAULT_MIN_TRAIN_SESSIONS,
    test_sessions: int = DEFAULT_TEST_SESSIONS,
    step_sessions: int = DEFAULT_STEP_SESSIONS,
    round_trip_cost_bps: float = DEFAULT_ROUND_TRIP_COST_BPS,
) -> MomentumFalsifierResult:
    """Run a deterministic walk-forward top-momentum falsifier.

    The Phase 1 model is intentionally thin: on each test date, rank available
    securities by persisted ``momentum_12_1`` value, select the top half, and
    compare its future return to an equal-weight universe baseline after the
    same per-rebalance round-trip cost assumption.
    """

    _validate_horizon(horizon_sessions)
    _validate_positive_int(min_train_sessions, "min_train_sessions")
    _validate_positive_int(test_sessions, "test_sessions")
    _validate_positive_int(step_sessions, "step_sessions")
    _validate_cost(round_trip_cost_bps)

    normalized_rows = _normalize_rows(rows)
    if not normalized_rows:
        return _insufficient_result(
            horizon_sessions=horizon_sessions,
            round_trip_cost_bps=round_trip_cost_bps,
            min_train_sessions=min_train_sessions,
            test_sessions=test_sessions,
            step_sessions=step_sessions,
            failure_modes=("No feature/label rows were available to score.",),
        )

    config = WalkForwardConfig(
        min_train_sessions=min_train_sessions,
        test_sessions=test_sessions,
        step_sessions=step_sessions,
        label_horizon_sessions=horizon_sessions,
    )
    calendar_rows = _calendar_rows_covering_inputs(calendar, normalized_rows)
    splits = plan_walk_forward_splits(calendar_rows, config)
    if not splits:
        return _insufficient_result(
            horizon_sessions=horizon_sessions,
            round_trip_cost_bps=round_trip_cost_bps,
            min_train_sessions=min_train_sessions,
            test_sessions=test_sessions,
            step_sessions=step_sessions,
            failure_modes=(
                "Not enough covered trading sessions to form one "
                "horizon-aware walk-forward split.",
            ),
        )

    rows_by_date = _rows_by_date(normalized_rows)
    windows: list[MomentumWindowResult] = []
    skipped_without_train = 0
    skipped_without_test = 0
    cost_fraction = round_trip_cost_bps / 10_000.0

    for split in splits:
        train_observations = sum(
            len(rows_by_date.get(session, ())) for session in split.train_sessions
        )
        test_observations = sum(
            len(rows_by_date.get(session, ())) for session in split.test_sessions
        )
        if train_observations == 0:
            skipped_without_train += 1
            continue
        if test_observations == 0:
            skipped_without_test += 1
            continue

        date_results = tuple(
            _score_test_date(rows_by_date[session], cost_fraction)
            for session in split.test_sessions
            if session in rows_by_date
        )
        if not date_results:
            skipped_without_test += 1
            continue

        windows.append(
            MomentumWindowResult(
                split_index=split.index,
                train_start=split.train_start,
                train_end=split.train_end,
                test_start=split.test_start,
                test_end=split.test_end,
                train_observations=train_observations,
                test_observations=test_observations,
                scored_dates=len(date_results),
                selected_observations=sum(
                    len(result.selected_tickers) for result in date_results
                ),
                strategy_net_return=_mean(
                    result.strategy_net_return for result in date_results
                ),
                baseline_net_return=_mean(
                    result.baseline_net_return for result in date_results
                ),
                date_results=date_results,
            )
        )

    headline = _headline_metrics(windows)
    failure_modes = _failure_modes(
        headline,
        skipped_without_train=skipped_without_train,
        skipped_without_test=skipped_without_test,
    )
    status: MomentumFalsifierStatus = "succeeded" if windows else "insufficient_data"
    if not windows:
        failure_modes = (
            "No scorable walk-forward windows remained after applying "
            "train/test data requirements.",
            *failure_modes,
        )

    return MomentumFalsifierResult(
        status=status,
        horizon_sessions=horizon_sessions,
        round_trip_cost_bps=round_trip_cost_bps,
        min_train_sessions=min_train_sessions,
        test_sessions=test_sessions,
        step_sessions=step_sessions,
        windows=tuple(windows),
        headline_metrics=headline,
        failure_modes=failure_modes,
    )


def _score_test_date(
    rows: Sequence[MomentumBacktestRow],
    cost_fraction: float,
) -> MomentumDateResult:
    ordered = tuple(sorted(rows, key=lambda row: (-row.feature_value, row.ticker)))
    selected_count = max(1, len(ordered) // 2)
    selected = ordered[:selected_count]

    strategy_gross = _mean(row.realized_return for row in selected)
    baseline_gross = _mean(row.realized_return for row in ordered)
    assert strategy_gross is not None
    assert baseline_gross is not None

    return MomentumDateResult(
        asof_date=ordered[0].asof_date,
        eligible_count=len(ordered),
        selected_tickers=tuple(row.ticker for row in selected),
        strategy_gross_return=strategy_gross,
        strategy_net_return=strategy_gross - cost_fraction,
        baseline_gross_return=baseline_gross,
        baseline_net_return=baseline_gross - cost_fraction,
    )


def _headline_metrics(
    windows: Sequence[MomentumWindowResult],
) -> MomentumHeadlineMetrics:
    date_results = tuple(
        date_result
        for window in windows
        for date_result in window.date_results
    )
    strategy_gross = tuple(result.strategy_gross_return for result in date_results)
    strategy_net = tuple(result.strategy_net_return for result in date_results)
    baseline_gross = tuple(result.baseline_gross_return for result in date_results)
    baseline_net = tuple(result.baseline_net_return for result in date_results)
    net_diff = tuple(
        strategy - baseline
        for strategy, baseline in zip(strategy_net, baseline_net, strict=True)
    )
    strategy_stddev = _sample_stddev(strategy_net)
    mean_strategy_net = _mean(strategy_net)
    return MomentumHeadlineMetrics(
        split_count=len(windows),
        scored_test_dates=len(date_results),
        eligible_observations=sum(result.eligible_count for result in date_results),
        selected_observations=sum(
            len(result.selected_tickers) for result in date_results
        ),
        mean_strategy_gross_return=_mean(strategy_gross),
        mean_strategy_net_return=mean_strategy_net,
        mean_baseline_gross_return=_mean(baseline_gross),
        mean_baseline_net_return=_mean(baseline_net),
        mean_net_difference_vs_baseline=_mean(net_diff),
        strategy_net_hit_rate=_hit_rate(strategy_net),
        strategy_net_return_stddev=strategy_stddev,
        strategy_net_return_to_stddev=(
            mean_strategy_net / strategy_stddev
            if mean_strategy_net is not None
            and strategy_stddev is not None
            and strategy_stddev > 0
            else None
        ),
    )


def _failure_modes(
    headline: MomentumHeadlineMetrics,
    *,
    skipped_without_train: int,
    skipped_without_test: int,
) -> tuple[str, ...]:
    failure_modes: list[str] = []
    if skipped_without_train:
        failure_modes.append(
            f"{skipped_without_train} walk-forward window(s) had no training rows."
        )
    if skipped_without_test:
        failure_modes.append(
            f"{skipped_without_test} walk-forward window(s) had no test rows."
        )
    if headline.scored_test_dates and headline.scored_test_dates < 5:
        failure_modes.append(
            "Scored sample is very small; treat metrics as plumbing evidence only."
        )
    if (
        headline.mean_strategy_net_return is not None
        and headline.mean_baseline_net_return is not None
        and headline.mean_strategy_net_return <= headline.mean_baseline_net_return
    ):
        failure_modes.append(
            "Momentum net mean did not exceed the equal-weight universe baseline."
        )
    return tuple(failure_modes)


def _insufficient_result(
    *,
    horizon_sessions: int,
    round_trip_cost_bps: float,
    min_train_sessions: int,
    test_sessions: int,
    step_sessions: int,
    failure_modes: tuple[str, ...],
) -> MomentumFalsifierResult:
    return MomentumFalsifierResult(
        status="insufficient_data",
        horizon_sessions=horizon_sessions,
        round_trip_cost_bps=round_trip_cost_bps,
        min_train_sessions=min_train_sessions,
        test_sessions=test_sessions,
        step_sessions=step_sessions,
        windows=(),
        headline_metrics=_headline_metrics(()),
        failure_modes=failure_modes,
    )


def _normalize_rows(
    rows: Sequence[MomentumBacktestRow],
) -> tuple[MomentumBacktestRow, ...]:
    if isinstance(rows, (str, bytes)) or not isinstance(rows, Sequence):
        raise MomentumFalsifierInputError("rows must be a sequence of MomentumBacktestRow")

    normalized_rows = tuple(rows)
    seen: set[tuple[str, date]] = set()
    for index, row in enumerate(normalized_rows, start=1):
        if not isinstance(row, MomentumBacktestRow):
            raise MomentumFalsifierInputError(
                f"rows[{index}] must be a MomentumBacktestRow"
            )
        _validate_row(row, index)
        key = (row.ticker, row.asof_date)
        if key in seen:
            raise MomentumFalsifierInputError(
                "duplicate feature/label row for "
                f"{row.ticker} on {row.asof_date.isoformat()}"
            )
        seen.add(key)

    return tuple(sorted(normalized_rows, key=lambda row: (row.asof_date, row.ticker)))


def _validate_row(row: MomentumBacktestRow, index: int) -> None:
    if not isinstance(row.ticker, str) or not row.ticker.strip():
        raise MomentumFalsifierInputError(f"rows[{index}].ticker must be non-empty")
    _validate_date(row.asof_date, f"rows[{index}].asof_date")
    _validate_date(row.horizon_date, f"rows[{index}].horizon_date")
    if row.horizon_date <= row.asof_date:
        raise MomentumFalsifierInputError(
            f"rows[{index}].horizon_date must be after asof_date"
        )
    if not isinstance(row.feature_value, float) or not math.isfinite(row.feature_value):
        raise MomentumFalsifierInputError(
            f"rows[{index}].feature_value must be a finite float"
        )
    if not isinstance(row.realized_return, float) or not math.isfinite(
        row.realized_return
    ):
        raise MomentumFalsifierInputError(
            f"rows[{index}].realized_return must be a finite float"
        )


def _validate_date(value: object, field_name: str) -> None:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise MomentumFalsifierInputError(f"{field_name} must be a date")


def _validate_horizon(horizon_sessions: int) -> None:
    if horizon_sessions not in CANONICAL_HORIZONS:
        allowed = ", ".join(str(horizon) for horizon in CANONICAL_HORIZONS)
        raise MomentumFalsifierInputError(
            f"horizon must be one of {allowed}; got {horizon_sessions}"
        )


def _validate_positive_int(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise MomentumFalsifierInputError(f"{field_name} must be a positive integer")


def _validate_cost(value: float) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MomentumFalsifierInputError("round_trip_cost_bps must be numeric")
    if not math.isfinite(float(value)) or value < 0:
        raise MomentumFalsifierInputError(
            "round_trip_cost_bps must be a non-negative finite number"
        )


def _calendar_rows_covering_inputs(
    calendar: TradingCalendar,
    rows: Sequence[MomentumBacktestRow],
):
    start = min(row.asof_date for row in rows)
    end = max(row.horizon_date for row in rows)
    return tuple(row for row in calendar.rows if start <= row.date <= end)


def _rows_by_date(
    rows: Sequence[MomentumBacktestRow],
) -> dict[date, tuple[MomentumBacktestRow, ...]]:
    grouped: dict[date, list[MomentumBacktestRow]] = {}
    for row in rows:
        grouped.setdefault(row.asof_date, []).append(row)
    return {
        asof_date: tuple(sorted(values, key=lambda row: row.ticker))
        for asof_date, values in grouped.items()
    }


def _mean(values: Sequence[float] | object) -> float | None:
    tuple_values = tuple(values)
    if not tuple_values:
        return None
    return sum(tuple_values) / len(tuple_values)


def _sample_stddev(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    average = _mean(values)
    assert average is not None
    variance = sum((value - average) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def _hit_rate(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return sum(1 for value in values if value > 0) / len(values)
