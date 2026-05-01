"""Point-in-time adjusted-close return feature families."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, DivisionByZero, InvalidOperation
from types import MappingProxyType
from typing import Literal

from silver.features.momentum_12_1 import (
    DAILY_PRICE_POLICY_NAME,
    DAILY_PRICE_POLICY_VERSION,
    DAILY_PRICE_TIMEZONE,
    AdjustedDailyPriceObservation,
    NumericFeatureDefinition,
    daily_price_available_at,
)
from silver.time.trading_calendar import TradingCalendar


FEATURE_VERSION = 1
MOMENTUM_6_1_LOOKBACK_SESSIONS = 126
RETURN_63_0_LOOKBACK_SESSIONS = 63
RETURN_21_0_LOOKBACK_SESSIONS = 21
SKIP_RECENT_MONTH_SESSIONS = 21
NO_SKIP_SESSIONS = 0

PriceReturnStatus = Literal["ok", "insufficient_history", "missing_price"]


class PriceReturnInputError(ValueError):
    """Raised when inputs cannot produce a deterministic price-return feature."""


@dataclass(frozen=True, slots=True)
class PriceReturnWindow:
    """Trading-session boundary selected for an adjusted-close return."""

    anchor_date: date | None
    start_date: date | None
    end_date: date | None
    lookback_sessions: int
    skip_recent_sessions: int
    start_available_at: datetime | None = None
    end_available_at: datetime | None = None
    missing_price_dates: tuple[date, ...] = ()


@dataclass(frozen=True, slots=True)
class PriceReturnFeatureValue:
    """Typed price-return feature value ready to map to feature_values."""

    security_id: int
    asof_date: date
    available_at: datetime
    definition: NumericFeatureDefinition
    value: float | None
    status: PriceReturnStatus
    window: PriceReturnWindow


@dataclass(frozen=True, slots=True)
class _VisibleAdjustedPrice:
    adjusted_close: Decimal
    effective_available_at: datetime


MOMENTUM_6_1_DEFINITION = NumericFeatureDefinition(
    name="momentum_6_1",
    version=FEATURE_VERSION,
    kind="numeric",
    computation_spec=MappingProxyType(
        {
            "adjusted_price_field": "adj_close",
            "return_type": "simple_return",
            "lookback_sessions": MOMENTUM_6_1_LOOKBACK_SESSIONS,
            "skip_recent_sessions": SKIP_RECENT_MONTH_SESSIONS,
            "daily_price_policy_name": DAILY_PRICE_POLICY_NAME,
            "daily_price_policy_version": DAILY_PRICE_POLICY_VERSION,
        }
    ),
)

RETURN_63_0_DEFINITION = NumericFeatureDefinition(
    name="return_63_0",
    version=FEATURE_VERSION,
    kind="numeric",
    computation_spec=MappingProxyType(
        {
            "adjusted_price_field": "adj_close",
            "return_type": "simple_return",
            "lookback_sessions": RETURN_63_0_LOOKBACK_SESSIONS,
            "skip_recent_sessions": NO_SKIP_SESSIONS,
            "daily_price_policy_name": DAILY_PRICE_POLICY_NAME,
            "daily_price_policy_version": DAILY_PRICE_POLICY_VERSION,
        }
    ),
)

RETURN_21_0_DEFINITION = NumericFeatureDefinition(
    name="return_21_0",
    version=FEATURE_VERSION,
    kind="numeric",
    computation_spec=MappingProxyType(
        {
            "adjusted_price_field": "adj_close",
            "return_type": "simple_return",
            "lookback_sessions": RETURN_21_0_LOOKBACK_SESSIONS,
            "skip_recent_sessions": NO_SKIP_SESSIONS,
            "daily_price_policy_name": DAILY_PRICE_POLICY_NAME,
            "daily_price_policy_version": DAILY_PRICE_POLICY_VERSION,
        }
    ),
)


def compute_momentum_6_1(
    *,
    security_id: int,
    asof: datetime,
    prices: Sequence[AdjustedDailyPriceObservation],
    calendar: TradingCalendar,
) -> PriceReturnFeatureValue:
    return compute_price_return(
        security_id=security_id,
        asof=asof,
        prices=prices,
        calendar=calendar,
        definition=MOMENTUM_6_1_DEFINITION,
        lookback_sessions=MOMENTUM_6_1_LOOKBACK_SESSIONS,
        skip_recent_sessions=SKIP_RECENT_MONTH_SESSIONS,
    )


def compute_return_63_0(
    *,
    security_id: int,
    asof: datetime,
    prices: Sequence[AdjustedDailyPriceObservation],
    calendar: TradingCalendar,
) -> PriceReturnFeatureValue:
    return compute_price_return(
        security_id=security_id,
        asof=asof,
        prices=prices,
        calendar=calendar,
        definition=RETURN_63_0_DEFINITION,
        lookback_sessions=RETURN_63_0_LOOKBACK_SESSIONS,
        skip_recent_sessions=NO_SKIP_SESSIONS,
    )


def compute_return_21_0(
    *,
    security_id: int,
    asof: datetime,
    prices: Sequence[AdjustedDailyPriceObservation],
    calendar: TradingCalendar,
) -> PriceReturnFeatureValue:
    return compute_price_return(
        security_id=security_id,
        asof=asof,
        prices=prices,
        calendar=calendar,
        definition=RETURN_21_0_DEFINITION,
        lookback_sessions=RETURN_21_0_LOOKBACK_SESSIONS,
        skip_recent_sessions=NO_SKIP_SESSIONS,
    )


def compute_price_return(
    *,
    security_id: int,
    asof: datetime,
    prices: Sequence[AdjustedDailyPriceObservation],
    calendar: TradingCalendar,
    definition: NumericFeatureDefinition,
    lookback_sessions: int,
    skip_recent_sessions: int,
) -> PriceReturnFeatureValue:
    """Compute an adjusted-close simple return using only rows visible at asof."""
    _require_aware(asof, "asof")
    _validate_offsets(
        lookback_sessions=lookback_sessions,
        skip_recent_sessions=skip_recent_sessions,
    )
    asof_date = asof.astimezone(DAILY_PRICE_TIMEZONE).date()
    anchor_date = _latest_policy_visible_session(calendar=calendar, asof=asof)
    if anchor_date is None:
        return _null_value(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            definition=definition,
            status="insufficient_history",
            window=PriceReturnWindow(
                None,
                None,
                None,
                lookback_sessions,
                skip_recent_sessions,
            ),
        )

    start_date = _session_offset(
        calendar=calendar,
        anchor_date=anchor_date,
        offset=lookback_sessions,
    )
    end_date = _session_offset(
        calendar=calendar,
        anchor_date=anchor_date,
        offset=skip_recent_sessions,
    )
    if start_date is None or end_date is None:
        return _null_value(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            definition=definition,
            status="insufficient_history",
            window=PriceReturnWindow(
                anchor_date,
                start_date,
                end_date,
                lookback_sessions,
                skip_recent_sessions,
            ),
        )

    visible_prices = _visible_prices_by_date(prices, asof)
    missing_dates = tuple(
        price_date
        for price_date in (start_date, end_date)
        if price_date not in visible_prices
    )
    if missing_dates:
        return _null_value(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            definition=definition,
            status="missing_price",
            window=PriceReturnWindow(
                anchor_date,
                start_date,
                end_date,
                lookback_sessions,
                skip_recent_sessions,
                missing_price_dates=missing_dates,
            ),
        )

    start_price = visible_prices[start_date]
    end_price = visible_prices[end_date]
    return PriceReturnFeatureValue(
        security_id=security_id,
        asof_date=asof_date,
        available_at=asof,
        definition=definition,
        value=_simple_return(
            start_price=start_price.adjusted_close,
            end_price=end_price.adjusted_close,
        ),
        status="ok",
        window=PriceReturnWindow(
            anchor_date=anchor_date,
            start_date=start_date,
            end_date=end_date,
            lookback_sessions=lookback_sessions,
            skip_recent_sessions=skip_recent_sessions,
            start_available_at=start_price.effective_available_at,
            end_available_at=end_price.effective_available_at,
        ),
    )


def _null_value(
    *,
    security_id: int,
    asof: datetime,
    asof_date: date,
    definition: NumericFeatureDefinition,
    status: Literal["insufficient_history", "missing_price"],
    window: PriceReturnWindow,
) -> PriceReturnFeatureValue:
    return PriceReturnFeatureValue(
        security_id=security_id,
        asof_date=asof_date,
        available_at=asof,
        definition=definition,
        value=None,
        status=status,
        window=window,
    )


def _latest_policy_visible_session(
    *,
    calendar: TradingCalendar,
    asof: datetime,
) -> date | None:
    if not calendar.rows:
        return None

    first_calendar_date = calendar.rows[0].date
    current = asof.astimezone(DAILY_PRICE_TIMEZONE).date()
    while current >= first_calendar_date:
        row = calendar.row_for(current)
        if row.is_session and daily_price_available_at(current) <= asof:
            return current
        current -= timedelta(days=1)
    return None


def _session_offset(
    *,
    calendar: TradingCalendar,
    anchor_date: date,
    offset: int,
) -> date | None:
    first_calendar_date = calendar.rows[0].date
    current = anchor_date
    sessions_seen = 0
    while sessions_seen < offset:
        current -= timedelta(days=1)
        if current < first_calendar_date:
            return None

        row = calendar.row_for(current)
        if row.is_session:
            sessions_seen += 1
    return current


def _visible_prices_by_date(
    prices: Sequence[AdjustedDailyPriceObservation],
    asof: datetime,
) -> dict[date, _VisibleAdjustedPrice]:
    visible_prices: dict[date, _VisibleAdjustedPrice] = {}
    seen_dates: set[date] = set()
    for price in prices:
        _validate_price(price)
        if price.price_date in seen_dates:
            raise PriceReturnInputError(
                f"duplicate adjusted daily price for {price.price_date.isoformat()}"
            )
        seen_dates.add(price.price_date)

        effective_available_at = _effective_price_available_at(price)
        if effective_available_at <= asof:
            visible_prices[price.price_date] = _VisibleAdjustedPrice(
                adjusted_close=price.adjusted_close,
                effective_available_at=effective_available_at,
            )
    return visible_prices


def _effective_price_available_at(price: AdjustedDailyPriceObservation) -> datetime:
    policy_available_at = daily_price_available_at(price.price_date)
    if price.available_at >= policy_available_at:
        return price.available_at
    return policy_available_at


def _simple_return(*, start_price: Decimal, end_price: Decimal) -> float:
    try:
        value = end_price / start_price - Decimal("1")
    except (DivisionByZero, InvalidOperation) as exc:
        raise PriceReturnInputError("adjusted_close must be non-zero") from exc
    return float(value)


def _validate_price(price: AdjustedDailyPriceObservation) -> None:
    if price.adjusted_close <= 0:
        raise PriceReturnInputError("adjusted_close must be positive")
    _require_aware(price.available_at, "price.available_at")


def _validate_offsets(*, lookback_sessions: int, skip_recent_sessions: int) -> None:
    if lookback_sessions <= 0:
        raise PriceReturnInputError("lookback_sessions must be positive")
    if skip_recent_sessions < 0:
        raise PriceReturnInputError("skip_recent_sessions must be non-negative")
    if skip_recent_sessions >= lookback_sessions:
        raise PriceReturnInputError(
            "skip_recent_sessions must be less than lookback_sessions"
        )


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise PriceReturnInputError(f"{field_name} must be timezone-aware")
