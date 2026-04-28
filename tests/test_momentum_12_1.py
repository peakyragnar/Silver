from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from silver.features.momentum_12_1 import (
    MOMENTUM_12_1_DEFINITION,
    AdjustedDailyPriceObservation,
    compute_momentum_12_1,
    daily_price_available_at,
)
from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


NY = ZoneInfo("America/New_York")
SECURITY_ID = 101


def test_compute_momentum_12_1_returns_typed_feature_value() -> None:
    calendar, sessions = _calendar_with_sessions(date(2023, 1, 3), session_count=253)
    anchor_date = sessions[-1]
    start_date = sessions[-253]
    end_date = sessions[-22]
    asof = _ny_datetime(anchor_date, 19)

    result = compute_momentum_12_1(
        security_id=SECURITY_ID,
        asof=asof,
        prices=[
            _price(start_date, "100.00"),
            _price(end_date, "133.33"),
            _price(anchor_date, "9999.00"),
        ],
        calendar=calendar,
    )

    expected_decimal = Decimal("133.33") / Decimal("100.00") - Decimal("1")
    assert result.security_id == SECURITY_ID
    assert result.asof_date == anchor_date
    assert result.definition == MOMENTUM_12_1_DEFINITION
    assert result.definition.name == "momentum_12_1"
    assert result.definition.version == 1
    assert result.definition.kind == "numeric"
    assert result.value == float(expected_decimal)
    assert result.status == "ok"
    assert result.window.anchor_date == anchor_date
    assert result.window.start_date == start_date
    assert result.window.end_date == end_date


def test_long_lookback_uses_trading_sessions_not_calendar_days() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=253)
    anchor_date = sessions[-1]
    start_date = sessions[-253]
    end_date = sessions[-22]
    calendar_day_start = anchor_date - timedelta(days=252)

    result = compute_momentum_12_1(
        security_id=SECURITY_ID,
        asof=_ny_datetime(anchor_date, 19),
        prices=[
            _price(start_date, "80.00"),
            _price(end_date, "120.00"),
            _price(calendar_day_start, "1.00"),
        ],
        calendar=calendar,
    )

    assert start_date != calendar_day_start
    assert result.value == 0.5
    assert result.window.start_date == start_date
    assert result.window.end_date == end_date


def test_insufficient_history_returns_null_feature_value() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=252)
    anchor_date = sessions[-1]

    result = compute_momentum_12_1(
        security_id=SECURITY_ID,
        asof=_ny_datetime(anchor_date, 19),
        prices=[],
        calendar=calendar,
    )

    assert result.value is None
    assert result.status == "insufficient_history"
    assert result.window.anchor_date == anchor_date
    assert result.window.start_date is None
    assert result.window.end_date is None


def test_missing_required_boundary_price_returns_null_feature_value() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=253)
    anchor_date = sessions[-1]
    start_date = sessions[-253]
    end_date = sessions[-22]

    result = compute_momentum_12_1(
        security_id=SECURITY_ID,
        asof=_ny_datetime(anchor_date, 19),
        prices=[_price(start_date, "100.00")],
        calendar=calendar,
    )

    assert result.value is None
    assert result.status == "missing_price"
    assert result.window.start_date == start_date
    assert result.window.end_date == end_date
    assert result.window.missing_price_dates == (end_date,)


def test_unavailable_asof_close_does_not_shift_momentum_window() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=254)
    current_date = sessions[-1]
    previous_visible_date = sessions[-2]

    previous_start = sessions[-254]
    previous_end = sessions[-23]
    current_start = sessions[-253]
    current_end = sessions[-22]
    prices = [
        _price(previous_start, "100.00"),
        _price(previous_end, "110.00"),
        _price(current_start, "200.00"),
        _price(current_end, "500.00"),
        _price(current_date, "9999.00"),
    ]

    before_close_result = compute_momentum_12_1(
        security_id=SECURITY_ID,
        asof=_ny_datetime(current_date, 12),
        prices=prices,
        calendar=calendar,
    )
    after_close_result = compute_momentum_12_1(
        security_id=SECURITY_ID,
        asof=_ny_datetime(current_date, 19),
        prices=prices,
        calendar=calendar,
    )

    assert before_close_result.value == 0.1
    assert before_close_result.window.anchor_date == previous_visible_date
    assert before_close_result.window.start_date == previous_start
    assert before_close_result.window.end_date == previous_end
    assert after_close_result.value == 1.5
    assert after_close_result.window.anchor_date == current_date
    assert after_close_result.window.start_date == current_start
    assert after_close_result.window.end_date == current_end


def test_price_math_uses_decimal_until_double_precision_boundary() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=253)
    anchor_date = sessions[-1]
    start_price = Decimal("97.13")
    end_price = Decimal("123.47")

    result = compute_momentum_12_1(
        security_id=SECURITY_ID,
        asof=_ny_datetime(anchor_date, 19),
        prices=[
            _price(sessions[-253], start_price),
            _price(sessions[-22], end_price),
        ],
        calendar=calendar,
    )

    # Feature-store values are double precision, but adjusted-close math stays
    # Decimal until the final conversion so the boundary behavior is explicit.
    expected_decimal = end_price / start_price - Decimal("1")
    assert result.value == float(expected_decimal)


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
    adjusted_close: str | Decimal,
) -> AdjustedDailyPriceObservation:
    return AdjustedDailyPriceObservation(
        price_date=price_date,
        adjusted_close=Decimal(adjusted_close),
        available_at=daily_price_available_at(price_date),
    )
