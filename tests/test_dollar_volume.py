from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from silver.features.dollar_volume import (
    AVG_DOLLAR_VOLUME_63_DEFINITION,
    AdjustedPriceVolumeObservation,
    compute_avg_dollar_volume_63,
    daily_price_available_at,
)
from silver.features.repository import feature_definition_hash
from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


NY = ZoneInfo("America/New_York")
SECURITY_ID = 101


def test_compute_avg_dollar_volume_63_returns_typed_numeric_feature() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=63)
    anchor_date = sessions[-1]
    window_dates = tuple(sessions[-63:])
    observations = tuple(
        _observation(day, adjusted_close="10.00", volume=1_000 + index)
        for index, day in enumerate(window_dates)
    )

    result = compute_avg_dollar_volume_63(
        security_id=SECURITY_ID,
        asof=_ny_datetime(anchor_date, 19),
        observations=observations,
        calendar=calendar,
    )

    expected = sum(
        Decimal("10.00") * Decimal(1_000 + index)
        for index in range(len(window_dates))
    ) / Decimal("63")
    assert result.security_id == SECURITY_ID
    assert result.asof_date == anchor_date
    assert result.definition == AVG_DOLLAR_VOLUME_63_DEFINITION
    assert result.definition.name == "avg_dollar_volume_63"
    assert result.definition.version == 1
    assert result.definition.kind == "numeric"
    assert result.definition.computation_spec["adjusted_price_field"] == "adj_close"
    assert result.definition.computation_spec["volume_field"] == "volume"
    assert (
        feature_definition_hash(result.definition)
        == "116d5d9771bb4b494c6c829527464c9beb7b62bce8b634cfce4af24dbdc34442"
    )
    assert result.value == float(expected)
    assert result.status == "ok"
    assert result.window.anchor_date == anchor_date
    assert result.window.start_date == window_dates[0]
    assert result.window.end_date == anchor_date
    assert result.window.observation_count == 63


def test_missing_price_returns_explicit_status() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=63)
    anchor_date = sessions[-1]
    missing_date = sessions[10]
    observations = tuple(
        _observation(day, adjusted_close="10.00", volume=1_000)
        for day in sessions
        if day != missing_date
    )

    result = compute_avg_dollar_volume_63(
        security_id=SECURITY_ID,
        asof=_ny_datetime(anchor_date, 19),
        observations=observations,
        calendar=calendar,
    )

    assert result.value is None
    assert result.status == "missing_price"
    assert result.window.missing_price_dates == (missing_date,)
    assert result.window.missing_volume_dates == ()


def test_missing_volume_returns_explicit_status() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=63)
    anchor_date = sessions[-1]
    missing_date = sessions[20]
    observations = tuple(
        _observation(
            day,
            adjusted_close="10.00",
            volume=None if day == missing_date else 1_000,
        )
        for day in sessions
    )

    result = compute_avg_dollar_volume_63(
        security_id=SECURITY_ID,
        asof=_ny_datetime(anchor_date, 19),
        observations=observations,
        calendar=calendar,
    )

    assert result.value is None
    assert result.status == "missing_volume"
    assert result.window.missing_price_dates == ()
    assert result.window.missing_volume_dates == (missing_date,)


def test_insufficient_history_returns_null_feature_value() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=62)
    anchor_date = sessions[-1]

    result = compute_avg_dollar_volume_63(
        security_id=SECURITY_ID,
        asof=_ny_datetime(anchor_date, 19),
        observations=[
            _observation(day, adjusted_close="10.00", volume=1_000)
            for day in sessions
        ],
        calendar=calendar,
    )

    assert result.value is None
    assert result.status == "insufficient_history"
    assert result.window.anchor_date == anchor_date
    assert result.window.start_date is None
    assert result.window.end_date is None


def test_unavailable_current_close_does_not_enter_window() -> None:
    calendar, sessions = _calendar_with_sessions(date(2024, 1, 2), session_count=64)
    current_date = sessions[-1]
    previous_visible_date = sessions[-2]
    previous_window = tuple(sessions[-64:-1])
    observations = [
        _observation(day, adjusted_close="10.00", volume=100)
        for day in previous_window
    ]
    observations.append(
        _observation(current_date, adjusted_close="999.00", volume=999_000)
    )

    result = compute_avg_dollar_volume_63(
        security_id=SECURITY_ID,
        asof=_ny_datetime(current_date, 12),
        observations=observations,
        calendar=calendar,
    )

    assert result.value == 1_000.0
    assert result.window.anchor_date == previous_visible_date
    assert result.window.start_date == previous_window[0]
    assert result.window.end_date == previous_visible_date


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


def _observation(
    price_date: date,
    *,
    adjusted_close: str | None,
    volume: int | None,
) -> AdjustedPriceVolumeObservation:
    return AdjustedPriceVolumeObservation(
        price_date=price_date,
        adjusted_close=Decimal(adjusted_close) if adjusted_close is not None else None,
        volume=volume,
        available_at=daily_price_available_at(price_date),
    )
