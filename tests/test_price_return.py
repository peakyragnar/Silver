from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from silver.features.momentum_12_1 import (
    AdjustedDailyPriceObservation,
    daily_price_available_at,
)
from silver.features.price_return import (
    MOMENTUM_6_1_DEFINITION,
    RETURN_21_0_DEFINITION,
    RETURN_63_0_DEFINITION,
    PriceReturnInputError,
    compute_momentum_6_1,
    compute_price_return,
    compute_return_21_0,
    compute_return_63_0,
)
from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


NY = ZoneInfo("America/New_York")
SECURITY_ID = 101


def test_compute_momentum_6_1_returns_typed_feature_value() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=127)
    anchor_date = sessions[-1]
    start_date = sessions[-127]
    end_date = sessions[-22]

    result = compute_momentum_6_1(
        security_id=SECURITY_ID,
        asof=_ny_datetime(anchor_date, 19),
        prices=[
            _price(start_date, "100.00"),
            _price(end_date, "125.00"),
            _price(anchor_date, "999.00"),
        ],
        calendar=calendar,
    )

    assert result.definition == MOMENTUM_6_1_DEFINITION
    assert result.definition.name == "momentum_6_1"
    assert result.value == 0.25
    assert result.status == "ok"
    assert result.window.anchor_date == anchor_date
    assert result.window.start_date == start_date
    assert result.window.end_date == end_date
    assert result.window.lookback_sessions == 126
    assert result.window.skip_recent_sessions == 21


def test_compute_return_63_0_uses_current_visible_close() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=64)
    anchor_date = sessions[-1]
    start_date = sessions[-64]

    result = compute_return_63_0(
        security_id=SECURITY_ID,
        asof=_ny_datetime(anchor_date, 19),
        prices=[
            _price(start_date, "100.00"),
            _price(anchor_date, "90.00"),
        ],
        calendar=calendar,
    )

    assert result.definition == RETURN_63_0_DEFINITION
    assert result.definition.name == "return_63_0"
    assert result.value == -0.1
    assert result.status == "ok"
    assert result.window.start_date == start_date
    assert result.window.end_date == anchor_date
    assert result.window.lookback_sessions == 63
    assert result.window.skip_recent_sessions == 0


def test_compute_return_21_0_before_close_uses_previous_anchor() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=23)
    current_date = sessions[-1]
    previous_anchor = sessions[-2]
    previous_start = sessions[-23]
    current_start = sessions[-22]
    prices = [
        _price(previous_start, "100.00"),
        _price(previous_anchor, "110.00"),
        _price(current_start, "200.00"),
        _price(current_date, "999.00"),
    ]

    before_close = compute_return_21_0(
        security_id=SECURITY_ID,
        asof=_ny_datetime(current_date, 12),
        prices=prices,
        calendar=calendar,
    )
    after_close = compute_return_21_0(
        security_id=SECURITY_ID,
        asof=_ny_datetime(current_date, 19),
        prices=prices,
        calendar=calendar,
    )

    assert before_close.definition == RETURN_21_0_DEFINITION
    assert before_close.value == 0.1
    assert before_close.window.anchor_date == previous_anchor
    assert before_close.window.start_date == previous_start
    assert before_close.window.end_date == previous_anchor
    assert after_close.value == 3.995
    assert after_close.window.anchor_date == current_date
    assert after_close.window.start_date == current_start
    assert after_close.window.end_date == current_date


def test_missing_boundary_price_returns_null_value() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=64)
    anchor_date = sessions[-1]

    result = compute_return_63_0(
        security_id=SECURITY_ID,
        asof=_ny_datetime(anchor_date, 19),
        prices=[_price(anchor_date, "100.00")],
        calendar=calendar,
    )

    assert result.value is None
    assert result.status == "missing_price"
    assert result.window.missing_price_dates == (sessions[-64],)


def test_invalid_offsets_are_rejected() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=64)

    with pytest.raises(PriceReturnInputError, match="skip_recent_sessions"):
        compute_price_return(
            security_id=SECURITY_ID,
            asof=_ny_datetime(sessions[-1], 19),
            prices=[],
            calendar=calendar,
            definition=RETURN_21_0_DEFINITION,
            lookback_sessions=21,
            skip_recent_sessions=21,
        )


def _calendar_with_sessions(
    start: date,
    *,
    session_count: int,
) -> tuple[TradingCalendar, list[date]]:
    rows: list[TradingCalendarRow] = []
    sessions: list[date] = []
    current = start
    while len(sessions) < session_count:
        is_session = current.weekday() < 5
        rows.append(
            TradingCalendarRow(
                date=current,
                is_session=is_session,
                session_close=_session_close(current) if is_session else None,
            )
        )
        if is_session:
            sessions.append(current)
        current += timedelta(days=1)
    return TradingCalendar(rows), sessions


def _session_close(day: date) -> datetime:
    return datetime.combine(day, time(21), tzinfo=timezone.utc)


def _ny_datetime(day: date, hour: int) -> datetime:
    return datetime.combine(day, time(hour), tzinfo=NY)


def _price(
    price_date: date,
    adjusted_close: str,
) -> AdjustedDailyPriceObservation:
    return AdjustedDailyPriceObservation(
        price_date=price_date,
        adjusted_close=Decimal(adjusted_close),
        available_at=daily_price_available_at(price_date),
    )
