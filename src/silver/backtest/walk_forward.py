"""Deterministic walk-forward split planning for backtests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Sequence

from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


class WalkForwardConfigError(ValueError):
    """Raised when walk-forward split planning inputs are invalid."""


@dataclass(frozen=True)
class WalkForwardConfig:
    """Configuration for session-counted walk-forward train/test splits."""

    min_train_sessions: int
    test_sessions: int
    step_sessions: int
    label_horizon_sessions: int
    max_train_sessions: int | None = None

    def __post_init__(self) -> None:
        _require_positive_int(self.min_train_sessions, "min_train_sessions")
        _require_positive_int(self.test_sessions, "test_sessions")
        _require_positive_int(self.step_sessions, "step_sessions")
        _require_positive_int(self.label_horizon_sessions, "label_horizon_sessions")
        if self.max_train_sessions is None:
            return
        _require_positive_int(self.max_train_sessions, "max_train_sessions")
        if self.max_train_sessions < self.min_train_sessions:
            raise WalkForwardConfigError(
                "max_train_sessions must be >= min_train_sessions"
            )


@dataclass(frozen=True)
class WalkForwardSplit:
    """A PIT-safe train/test split over trading sessions.

    ``label_gap_sessions`` is the embargo between training and testing. The last
    training label outcome lands inside this gap, so all training labels are
    known before ``test_start``.
    """

    index: int
    train_sessions: tuple[date, ...]
    label_gap_sessions: tuple[date, ...]
    test_sessions: tuple[date, ...]
    label_horizon_sessions: int
    train_label_outcome_end: date
    test_label_outcome_end: date

    @property
    def train_start(self) -> date:
        return self.train_sessions[0]

    @property
    def train_end(self) -> date:
        return self.train_sessions[-1]

    @property
    def test_start(self) -> date:
        return self.test_sessions[0]

    @property
    def test_end(self) -> date:
        return self.test_sessions[-1]

    @property
    def train_labels_available_before(self) -> date:
        return self.test_start


def plan_walk_forward_splits(
    calendar: TradingCalendar | Sequence[TradingCalendarRow],
    config: WalkForwardConfig,
) -> tuple[WalkForwardSplit, ...]:
    """Return deterministic, horizon-aware walk-forward splits.

    Split boundaries are counted in trading sessions, not calendar days. The
    planner requires enough future calendar to score every test row at the
    configured label horizon. Each split has disjoint train/test sessions.
    Later splits may train on sessions that were tested in earlier splits only
    after those sessions' label outcomes are available before the new test
    window.
    """

    session_dates = _session_dates(calendar)
    earliest_test_start_index = (
        config.min_train_sessions + config.label_horizon_sessions
    )
    latest_test_start_index = (
        len(session_dates) - config.test_sessions - config.label_horizon_sessions
    )
    if earliest_test_start_index > latest_test_start_index:
        return ()

    splits: list[WalkForwardSplit] = []
    for test_start_index in range(
        earliest_test_start_index,
        latest_test_start_index + 1,
        config.step_sessions,
    ):
        train_end_index = test_start_index - config.label_horizon_sessions - 1
        train_start_index = _train_start_index(train_end_index, config)
        test_end_index = test_start_index + config.test_sessions - 1

        splits.append(
            WalkForwardSplit(
                index=len(splits),
                train_sessions=session_dates[train_start_index : train_end_index + 1],
                label_gap_sessions=session_dates[
                    train_end_index + 1 : test_start_index
                ],
                test_sessions=session_dates[test_start_index : test_end_index + 1],
                label_horizon_sessions=config.label_horizon_sessions,
                train_label_outcome_end=session_dates[
                    train_end_index + config.label_horizon_sessions
                ],
                test_label_outcome_end=session_dates[
                    test_end_index + config.label_horizon_sessions
                ],
            )
        )

    return tuple(splits)


def _train_start_index(train_end_index: int, config: WalkForwardConfig) -> int:
    if config.max_train_sessions is None:
        return 0
    return max(0, train_end_index - config.max_train_sessions + 1)


def _session_dates(
    calendar: TradingCalendar | Sequence[TradingCalendarRow],
) -> tuple[date, ...]:
    rows = calendar.rows if isinstance(calendar, TradingCalendar) else calendar
    sessions: list[date] = []
    seen: set[date] = set()

    for row in sorted(rows, key=lambda item: item.date):
        if not row.is_session:
            continue
        if row.date in seen:
            raise WalkForwardConfigError(
                f"duplicate session date in trading calendar: {row.date.isoformat()}"
            )
        seen.add(row.date)
        sessions.append(row.date)

    return tuple(sessions)


def _require_positive_int(value: int, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise WalkForwardConfigError(f"{field} must be a positive integer")
