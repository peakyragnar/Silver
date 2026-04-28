from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from silver.backtest import (
    WalkForwardConfig,
    WalkForwardConfigError,
    plan_walk_forward_splits,
)
from silver.time.trading_calendar import TradingCalendarRow


def test_expanding_splits_enforce_horizon_gap_and_test_label_coverage() -> None:
    rows = _synthetic_calendar(13)
    config = WalkForwardConfig(
        min_train_sessions=3,
        test_sessions=2,
        step_sessions=2,
        label_horizon_sessions=2,
    )

    splits = plan_walk_forward_splits(rows, config)

    assert [(split.train_start, split.train_end) for split in splits] == [
        (date(2024, 1, 1), date(2024, 1, 3)),
        (date(2024, 1, 1), date(2024, 1, 5)),
        (date(2024, 1, 1), date(2024, 1, 7)),
    ]
    assert [(split.test_start, split.test_end) for split in splits] == [
        (date(2024, 1, 6), date(2024, 1, 7)),
        (date(2024, 1, 8), date(2024, 1, 9)),
        (date(2024, 1, 10), date(2024, 1, 11)),
    ]
    assert [split.label_gap_sessions for split in splits] == [
        (date(2024, 1, 4), date(2024, 1, 5)),
        (date(2024, 1, 6), date(2024, 1, 7)),
        (date(2024, 1, 8), date(2024, 1, 9)),
    ]
    assert [
        (split.train_label_outcome_end, split.train_labels_available_before)
        for split in splits
    ] == [
        (date(2024, 1, 5), date(2024, 1, 6)),
        (date(2024, 1, 7), date(2024, 1, 8)),
        (date(2024, 1, 9), date(2024, 1, 10)),
    ]
    assert all(
        set(split.train_sessions).isdisjoint(split.test_sessions) for split in splits
    )
    assert set(splits[0].test_sessions).issubset(splits[2].train_sessions)
    assert splits[-1].test_label_outcome_end == date(2024, 1, 13)


def test_rolling_splits_keep_latest_training_window() -> None:
    rows = _synthetic_calendar(15)
    config = WalkForwardConfig(
        min_train_sessions=3,
        max_train_sessions=4,
        test_sessions=2,
        step_sessions=3,
        label_horizon_sessions=2,
    )

    splits = plan_walk_forward_splits(rows, config)

    assert [split.train_sessions for split in splits] == [
        (date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)),
        (date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5), date(2024, 1, 6)),
        (date(2024, 1, 6), date(2024, 1, 7), date(2024, 1, 8), date(2024, 1, 9)),
    ]


def test_short_calendars_return_no_splits() -> None:
    rows = _synthetic_calendar(6)
    config = WalkForwardConfig(
        min_train_sessions=3,
        test_sessions=2,
        step_sessions=1,
        label_horizon_sessions=2,
    )

    assert plan_walk_forward_splits(rows, config) == ()


def test_split_order_is_deterministic_for_unsorted_calendar_rows() -> None:
    rows = tuple(reversed(_synthetic_calendar(13)))
    config = WalkForwardConfig(
        min_train_sessions=3,
        test_sessions=2,
        step_sessions=2,
        label_horizon_sessions=2,
    )

    first = plan_walk_forward_splits(rows, config)
    second = plan_walk_forward_splits(rows, config)

    assert first == second
    assert [split.index for split in first] == [0, 1, 2]
    assert [split.test_start for split in first] == [
        date(2024, 1, 6),
        date(2024, 1, 8),
        date(2024, 1, 10),
    ]


def test_non_session_rows_are_ignored_when_planning() -> None:
    rows = [
        TradingCalendarRow(date(2024, 1, 2), True, _close("2024-01-02T21:00:00+00:00")),
        TradingCalendarRow(date(2024, 1, 1), False, None),
        TradingCalendarRow(date(2024, 1, 3), True, _close("2024-01-03T21:00:00+00:00")),
        TradingCalendarRow(date(2024, 1, 4), True, _close("2024-01-04T21:00:00+00:00")),
        TradingCalendarRow(date(2024, 1, 5), False, None),
        TradingCalendarRow(date(2024, 1, 8), True, _close("2024-01-08T21:00:00+00:00")),
        TradingCalendarRow(date(2024, 1, 9), True, _close("2024-01-09T21:00:00+00:00")),
        TradingCalendarRow(date(2024, 1, 10), True, _close("2024-01-10T21:00:00+00:00")),
    ]
    config = WalkForwardConfig(
        min_train_sessions=2,
        test_sessions=1,
        step_sessions=1,
        label_horizon_sessions=1,
    )

    splits = plan_walk_forward_splits(rows, config)

    assert [
        (split.train_end, split.label_gap_sessions, split.test_start)
        for split in splits
    ] == [
        (date(2024, 1, 3), (date(2024, 1, 4),), date(2024, 1, 8)),
        (date(2024, 1, 4), (date(2024, 1, 8),), date(2024, 1, 9)),
    ]


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"min_train_sessions": 0}, "min_train_sessions must be a positive integer"),
        ({"test_sessions": 0}, "test_sessions must be a positive integer"),
        ({"step_sessions": 0}, "step_sessions must be a positive integer"),
        (
            {"label_horizon_sessions": 0},
            "label_horizon_sessions must be a positive integer",
        ),
        (
            {"max_train_sessions": 2},
            "max_train_sessions must be >= min_train_sessions",
        ),
    ],
)
def test_bad_configs_fail_with_clear_errors(
    kwargs: dict[str, int],
    match: str,
) -> None:
    values = {
        "min_train_sessions": 3,
        "test_sessions": 2,
        "step_sessions": 1,
        "label_horizon_sessions": 2,
    }
    values.update(kwargs)

    with pytest.raises(WalkForwardConfigError, match=match):
        WalkForwardConfig(**values)


def test_duplicate_session_dates_fail_closed() -> None:
    rows = [
        TradingCalendarRow(date(2024, 1, 2), True, _close("2024-01-02T21:00:00+00:00")),
        TradingCalendarRow(date(2024, 1, 2), True, _close("2024-01-02T21:00:00+00:00")),
    ]
    config = WalkForwardConfig(
        min_train_sessions=1,
        test_sessions=1,
        step_sessions=1,
        label_horizon_sessions=1,
    )

    with pytest.raises(WalkForwardConfigError, match="duplicate session date"):
        plan_walk_forward_splits(rows, config)


def _synthetic_calendar(session_count: int) -> tuple[TradingCalendarRow, ...]:
    start = date(2024, 1, 1)
    return tuple(
        TradingCalendarRow(
            date=start + timedelta(days=offset),
            is_session=True,
            session_close=_close(f"2024-01-{offset + 1:02d}T21:00:00+00:00"),
        )
        for offset in range(session_count)
    )


def _close(value: str) -> datetime:
    return datetime.fromisoformat(value)
