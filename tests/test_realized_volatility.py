from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from silver.features.momentum_12_1 import (
    AdjustedDailyPriceObservation,
    daily_price_available_at,
)
from silver.features.realized_volatility import (
    ANNUALIZATION_SESSIONS,
    REALIZED_VOLATILITY_63_DEFINITION,
    RETURN_WINDOW_SESSIONS,
    compute_realized_volatility_63,
)
from silver.features.repository import (
    feature_definition_hash,
    feature_definition_payload,
)
from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


NY = ZoneInfo("America/New_York")
SECURITY_ID = 101


def test_compute_realized_volatility_63_returns_typed_feature_value() -> None:
    calendar, sessions = _calendar_with_sessions(
        date(2024, 1, 2),
        session_count=RETURN_WINDOW_SESSIONS + 1,
    )
    anchor_date = sessions[-1]
    window_dates = tuple(sessions[-(RETURN_WINDOW_SESSIONS + 1) :])
    returns = _varied_returns(RETURN_WINDOW_SESSIONS)

    result = compute_realized_volatility_63(
        security_id=SECURITY_ID,
        asof=_ny_datetime(anchor_date, 19),
        prices=_prices_from_returns(window_dates, returns),
        calendar=calendar,
    )

    assert result.security_id == SECURITY_ID
    assert result.asof_date == anchor_date
    assert result.definition == REALIZED_VOLATILITY_63_DEFINITION
    assert result.definition.name == "realized_volatility_63"
    assert result.definition.version == 1
    assert result.definition.kind == "numeric"
    assert result.status == "ok"
    assert result.window.anchor_date == anchor_date
    assert result.window.start_date == window_dates[0]
    assert result.window.end_date == anchor_date
    assert result.window.return_count == RETURN_WINDOW_SESSIONS
    assert result.window.missing_price_dates == ()
    assert result.value is not None
    assert math.isclose(
        result.value,
        _expected_annualized_sample_volatility(returns),
        rel_tol=1e-15,
    )


def test_definition_metadata_and_hash_are_stable() -> None:
    payload = feature_definition_payload(REALIZED_VOLATILITY_63_DEFINITION)

    assert payload == {
        "name": "realized_volatility_63",
        "version": 1,
        "kind": "numeric",
        "computation_spec": {
            "adjusted_price_field": "adj_close",
            "return_type": "daily_simple_return",
            "volatility_window_sessions": 63,
            "volatility_estimator": "sample_standard_deviation",
            "annualized": True,
            "annualization_sessions": 252,
            "daily_price_policy_name": "daily_price",
            "daily_price_policy_version": 1,
        },
    }
    assert (
        feature_definition_hash(REALIZED_VOLATILITY_63_DEFINITION)
        == "ec2947d4fd056e43751818c367f8ba79e7e665c36118c1abbe76bf0f61c77956"
    )


def test_insufficient_history_returns_null_feature_value() -> None:
    calendar, sessions = _calendar_with_sessions(
        date(2024, 1, 2),
        session_count=RETURN_WINDOW_SESSIONS,
    )
    anchor_date = sessions[-1]

    result = compute_realized_volatility_63(
        security_id=SECURITY_ID,
        asof=_ny_datetime(anchor_date, 19),
        prices=[],
        calendar=calendar,
    )

    assert result.value is None
    assert result.status == "insufficient_history"
    assert result.window.anchor_date == anchor_date
    assert result.window.start_date is None
    assert result.window.end_date == anchor_date


def test_missing_required_price_returns_null_feature_value() -> None:
    calendar, sessions = _calendar_with_sessions(
        date(2024, 1, 2),
        session_count=RETURN_WINDOW_SESSIONS + 1,
    )
    anchor_date = sessions[-1]
    window_dates = tuple(sessions[-(RETURN_WINDOW_SESSIONS + 1) :])
    missing_date = window_dates[17]
    prices = tuple(
        price
        for price in _prices_from_returns(
            window_dates,
            _varied_returns(RETURN_WINDOW_SESSIONS),
        )
        if price.price_date != missing_date
    )

    result = compute_realized_volatility_63(
        security_id=SECURITY_ID,
        asof=_ny_datetime(anchor_date, 19),
        prices=prices,
        calendar=calendar,
    )

    assert result.value is None
    assert result.status == "missing_price"
    assert result.window.start_date == window_dates[0]
    assert result.window.end_date == anchor_date
    assert result.window.missing_price_dates == (missing_date,)


def test_unavailable_asof_price_cannot_leak_into_realized_volatility() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=65)
    current_date = sessions[-1]
    previous_visible_date = sessions[-2]
    returns = (*_varied_returns(63), Decimal("0.50"))
    prices = _prices_from_returns(tuple(sessions), returns)

    before_close_result = compute_realized_volatility_63(
        security_id=SECURITY_ID,
        asof=_ny_datetime(current_date, 12),
        prices=prices,
        calendar=calendar,
    )
    after_close_result = compute_realized_volatility_63(
        security_id=SECURITY_ID,
        asof=_ny_datetime(current_date, 19),
        prices=prices,
        calendar=calendar,
    )

    assert before_close_result.window.anchor_date == previous_visible_date
    assert before_close_result.window.start_date == sessions[0]
    assert before_close_result.window.end_date == previous_visible_date
    assert before_close_result.value is not None
    assert math.isclose(
        before_close_result.value,
        _expected_annualized_sample_volatility(returns[:63]),
        rel_tol=1e-15,
    )
    assert after_close_result.window.anchor_date == current_date
    assert after_close_result.window.start_date == sessions[1]
    assert after_close_result.window.end_date == current_date
    assert after_close_result.value is not None
    assert math.isclose(
        after_close_result.value,
        _expected_annualized_sample_volatility(returns[1:]),
        rel_tol=1e-15,
    )
    assert before_close_result.value != after_close_result.value


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


def _prices_from_returns(
    price_dates: tuple[date, ...],
    returns: tuple[Decimal, ...],
) -> tuple[AdjustedDailyPriceObservation, ...]:
    assert len(price_dates) == len(returns) + 1
    adjusted_close = Decimal("100.00")
    prices = [_price(price_dates[0], adjusted_close)]
    for price_date, daily_return in zip(price_dates[1:], returns):
        adjusted_close *= Decimal("1") + daily_return
        prices.append(_price(price_date, adjusted_close))
    return tuple(prices)


def _price(
    price_date: date,
    adjusted_close: Decimal,
) -> AdjustedDailyPriceObservation:
    return AdjustedDailyPriceObservation(
        price_date=price_date,
        adjusted_close=adjusted_close,
        available_at=daily_price_available_at(price_date),
    )


def _varied_returns(count: int) -> tuple[Decimal, ...]:
    return tuple(Decimal((index % 7) - 3) / Decimal("1000") for index in range(count))


def _expected_annualized_sample_volatility(returns: tuple[Decimal, ...]) -> float:
    mean = sum(returns, Decimal("0")) / Decimal(len(returns))
    variance = sum((value - mean) ** 2 for value in returns) / (
        Decimal(len(returns)) - Decimal("1")
    )
    return math.sqrt(float(variance)) * math.sqrt(ANNUALIZATION_SESSIONS)
