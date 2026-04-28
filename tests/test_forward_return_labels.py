from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from silver.labels.forward_returns import (
    ForwardReturnLabelInputError,
    SkipReason,
    calculate_forward_return_labels,
)
from silver.prices.daily import DailyPriceRow
from silver.time.trading_calendar import (
    CANONICAL_HORIZONS,
    TradingCalendar,
    TradingCalendarRow,
)


def test_calculates_phase_1_labels_from_adjusted_closes_and_session_horizons() -> None:
    calendar = _calendar_with_sessions(260)
    sessions = _sessions(calendar)
    prices = _prices("AAA", sessions)

    result = calculate_forward_return_labels(
        prices=prices,
        calendar=calendar,
        asof_dates=[sessions[0]],
    )

    assert result.skipped == ()
    assert [label.horizon_days for label in result.labels] == list(CANONICAL_HORIZONS)
    for label in result.labels:
        target_session = sessions[label.horizon_days]
        assert label.security == "AAA"
        assert label.asof_date == sessions[0]
        assert label.target_date == target_session
        assert label.available_at == calendar.row_for(target_session).session_close
        assert label.asof_adj_close == Decimal("100")
        assert label.target_adj_close == Decimal(100 + label.horizon_days)
        assert label.forward_return == Decimal(label.horizon_days) / Decimal("100")
        assert label.benchmark_forward_return is None
        assert label.excess_return is None


def test_calculates_optional_benchmark_and_excess_returns() -> None:
    calendar = _calendar_with_sessions(260)
    sessions = _sessions(calendar)
    prices = _prices("AAA", sessions)
    benchmark_prices = _prices("SPY", sessions, start=Decimal("200"))

    result = calculate_forward_return_labels(
        prices=prices,
        calendar=calendar,
        asof_dates=[sessions[0]],
        horizons=[5],
        benchmark_prices=benchmark_prices,
    )

    assert result.skipped == ()
    assert len(result.labels) == 1
    label = result.labels[0]
    assert label.forward_return == Decimal("0.05")
    assert label.benchmark_forward_return == Decimal("0.025")
    assert label.excess_return == Decimal("0.025")


def test_missing_benchmark_price_does_not_zero_fill_excess_return() -> None:
    calendar = _calendar_with_sessions(260)
    sessions = _sessions(calendar)
    prices = _prices("AAA", sessions)
    benchmark_prices = [
        row for row in _prices("SPY", sessions, start=Decimal("200"))
        if row.date != sessions[5]
    ]

    result = calculate_forward_return_labels(
        prices=prices,
        calendar=calendar,
        asof_dates=[sessions[0]],
        horizons=[5],
        benchmark_prices=benchmark_prices,
    )

    assert result.skipped == ()
    assert len(result.labels) == 1
    label = result.labels[0]
    assert label.forward_return == Decimal("0.05")
    assert label.benchmark_forward_return is None
    assert label.excess_return is None


def test_missing_target_price_is_surfaced_without_fake_label() -> None:
    calendar = _calendar_with_sessions(260)
    sessions = _sessions(calendar)
    target_date = sessions[5]
    prices = [row for row in _prices("AAA", sessions) if row.date != target_date]

    result = calculate_forward_return_labels(
        prices=prices,
        calendar=calendar,
        asof_dates=[sessions[0]],
        horizons=[5],
    )

    assert result.labels == ()
    assert len(result.skipped) == 1
    skipped = result.skipped[0]
    assert skipped.security == "AAA"
    assert skipped.asof_date == sessions[0]
    assert skipped.horizon_days == 5
    assert skipped.target_date == target_date
    assert skipped.reason is SkipReason.MISSING_TARGET_PRICE


def test_missing_asof_price_is_surfaced_without_fake_label() -> None:
    calendar = _calendar_with_sessions(260)
    sessions = _sessions(calendar)
    prices = [row for row in _prices("AAA", sessions) if row.date != sessions[0]]

    result = calculate_forward_return_labels(
        prices=prices,
        calendar=calendar,
        asof_dates=[sessions[0]],
        horizons=[5],
    )

    assert result.labels == ()
    assert len(result.skipped) == 1
    skipped = result.skipped[0]
    assert skipped.reason is SkipReason.MISSING_ASOF_PRICE
    assert skipped.target_date is None


def test_insufficient_calendar_history_is_surfaced() -> None:
    calendar = _calendar_with_sessions(3)
    sessions = _sessions(calendar)
    prices = _prices("AAA", sessions)

    result = calculate_forward_return_labels(
        prices=prices,
        calendar=calendar,
        asof_dates=[sessions[0]],
        horizons=[5],
    )

    assert result.labels == ()
    assert len(result.skipped) == 1
    assert result.skipped[0].reason is SkipReason.INSUFFICIENT_CALENDAR_HISTORY


def test_non_session_asof_date_is_surfaced() -> None:
    calendar = _calendar_with_sessions(10)
    sessions = _sessions(calendar)
    non_session = date(2024, 1, 6)
    prices = _prices("AAA", sessions)

    result = calculate_forward_return_labels(
        prices=prices,
        calendar=calendar,
        asof_dates=[non_session],
        horizons=[5],
    )

    assert result.labels == ()
    assert len(result.skipped) == 1
    skipped = result.skipped[0]
    assert skipped.asof_date == non_session
    assert skipped.reason is SkipReason.NON_SESSION_ASOF
    assert skipped.target_date is None


def test_rejects_non_phase_1_horizons() -> None:
    calendar = _calendar_with_sessions(10)
    sessions = _sessions(calendar)

    with pytest.raises(ForwardReturnLabelInputError, match="5, 21, 63, 126, 252"):
        calculate_forward_return_labels(
            prices=_prices("AAA", sessions),
            calendar=calendar,
            asof_dates=[sessions[0]],
            horizons=[1],
        )


def test_results_are_deterministic_for_identical_inputs() -> None:
    calendar = _calendar_with_sessions(260)
    sessions = _sessions(calendar)
    prices = _prices("AAA", sessions) + _prices("BBB", sessions, start=Decimal("50"))

    first = calculate_forward_return_labels(
        prices=list(reversed(prices)),
        calendar=calendar,
        asof_dates=[sessions[0], sessions[1]],
        horizons=[5, 21],
    )
    second = calculate_forward_return_labels(
        prices=prices,
        calendar=calendar,
        asof_dates=[sessions[1], sessions[0]],
        horizons=[21, 5],
    )

    assert first == second


def _calendar_with_sessions(session_count: int) -> TradingCalendar:
    rows: list[TradingCalendarRow] = []
    current = date(2024, 1, 2)
    sessions_seen = 0
    while sessions_seen < session_count:
        is_session = current.weekday() < 5
        rows.append(
            TradingCalendarRow(
                date=current,
                is_session=is_session,
                session_close=(
                    datetime.combine(current, datetime.min.time(), tzinfo=timezone.utc)
                    + timedelta(hours=21)
                    if is_session
                    else None
                ),
            )
        )
        if is_session:
            sessions_seen += 1
        current += timedelta(days=1)
    return TradingCalendar(rows)


def _sessions(calendar: TradingCalendar) -> list[date]:
    return [row.date for row in calendar.rows if row.is_session]


def _prices(
    security: str,
    sessions: list[date],
    *,
    start: Decimal = Decimal("100"),
) -> list[DailyPriceRow]:
    return [
        DailyPriceRow(
            ticker=security,
            date=session,
            open=start + Decimal(index),
            high=start + Decimal(index),
            low=start + Decimal(index),
            close=start + Decimal("1000") + Decimal(index),
            adj_close=start + Decimal(index),
            volume=1000,
            source="fixture",
        )
        for index, session in enumerate(sessions)
    ]
