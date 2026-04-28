"""Point-in-time realized volatility from adjusted daily prices."""

from __future__ import annotations

import math
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


FEATURE_NAME = "realized_volatility_63"
FEATURE_VERSION = 1
RETURN_WINDOW_SESSIONS = 63
ANNUALIZATION_SESSIONS = 252

FeatureStatus = Literal["ok", "insufficient_history", "missing_price"]


class RealizedVolatilityInputError(ValueError):
    """Raised when inputs cannot produce deterministic realized volatility."""


@dataclass(frozen=True, slots=True)
class RealizedVolatilityWindow:
    """Trading-session window selected for a realized-volatility value."""

    anchor_date: date | None
    start_date: date | None
    end_date: date | None
    return_count: int = RETURN_WINDOW_SESSIONS
    start_available_at: datetime | None = None
    end_available_at: datetime | None = None
    missing_price_dates: tuple[date, ...] = ()


@dataclass(frozen=True, slots=True)
class RealizedVolatilityFeatureValue:
    """Typed realized-volatility value ready to map to feature_values later."""

    security_id: int
    asof_date: date
    available_at: datetime
    definition: NumericFeatureDefinition
    value: float | None
    status: FeatureStatus
    window: RealizedVolatilityWindow


@dataclass(frozen=True, slots=True)
class _VisibleAdjustedPrice:
    adjusted_close: Decimal
    effective_available_at: datetime


REALIZED_VOLATILITY_63_DEFINITION = NumericFeatureDefinition(
    name=FEATURE_NAME,
    version=FEATURE_VERSION,
    kind="numeric",
    computation_spec=MappingProxyType(
        {
            "adjusted_price_field": "adj_close",
            "return_type": "daily_simple_return",
            "volatility_window_sessions": RETURN_WINDOW_SESSIONS,
            "volatility_estimator": "sample_standard_deviation",
            "annualized": True,
            "annualization_sessions": ANNUALIZATION_SESSIONS,
            "daily_price_policy_name": DAILY_PRICE_POLICY_NAME,
            "daily_price_policy_version": DAILY_PRICE_POLICY_VERSION,
        }
    ),
)


def compute_realized_volatility_63(
    *,
    security_id: int,
    asof: datetime,
    prices: Sequence[AdjustedDailyPriceObservation],
    calendar: TradingCalendar,
) -> RealizedVolatilityFeatureValue:
    """Compute 63-session annualized realized volatility visible at ``asof``.

    The value is ``sqrt(252)`` times the sample standard deviation of 63 daily
    simple adjusted-close returns ending at the latest daily price whose policy
    availability is no later than ``asof``. Computing 63 returns requires 64
    visible adjusted-close observations.
    """
    _require_aware(asof, "asof")
    asof_date = asof.astimezone(DAILY_PRICE_TIMEZONE).date()
    anchor_date = _latest_policy_visible_session(calendar=calendar, asof=asof)
    if anchor_date is None:
        return _null_value(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            status="insufficient_history",
            window=RealizedVolatilityWindow(None, None, None),
        )

    window_dates = _window_session_dates(
        calendar=calendar,
        anchor_date=anchor_date,
        return_count=RETURN_WINDOW_SESSIONS,
    )
    if window_dates is None:
        return _null_value(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            status="insufficient_history",
            window=RealizedVolatilityWindow(anchor_date, None, anchor_date),
        )

    visible_prices = _visible_prices_by_date(prices, asof)
    missing_dates = tuple(
        price_date for price_date in window_dates if price_date not in visible_prices
    )
    if missing_dates:
        return _null_value(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            status="missing_price",
            window=RealizedVolatilityWindow(
                anchor_date=anchor_date,
                start_date=window_dates[0],
                end_date=window_dates[-1],
                missing_price_dates=missing_dates,
            ),
        )

    window_prices = tuple(
        visible_prices[price_date] for price_date in window_dates
    )
    returns = _daily_simple_returns(
        tuple(price.adjusted_close for price in window_prices)
    )
    return RealizedVolatilityFeatureValue(
        security_id=security_id,
        asof_date=asof_date,
        available_at=asof,
        definition=REALIZED_VOLATILITY_63_DEFINITION,
        value=_annualized_sample_standard_deviation(returns),
        status="ok",
        window=RealizedVolatilityWindow(
            anchor_date=anchor_date,
            start_date=window_dates[0],
            end_date=window_dates[-1],
            start_available_at=window_prices[0].effective_available_at,
            end_available_at=window_prices[-1].effective_available_at,
        ),
    )


def _null_value(
    *,
    security_id: int,
    asof: datetime,
    asof_date: date,
    status: Literal["insufficient_history", "missing_price"],
    window: RealizedVolatilityWindow,
) -> RealizedVolatilityFeatureValue:
    return RealizedVolatilityFeatureValue(
        security_id=security_id,
        asof_date=asof_date,
        available_at=asof,
        definition=REALIZED_VOLATILITY_63_DEFINITION,
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


def _window_session_dates(
    *,
    calendar: TradingCalendar,
    anchor_date: date,
    return_count: int,
) -> tuple[date, ...] | None:
    first_calendar_date = calendar.rows[0].date
    current = anchor_date
    dates = [anchor_date]
    while len(dates) < return_count + 1:
        current -= timedelta(days=1)
        if current < first_calendar_date:
            return None

        row = calendar.row_for(current)
        if row.is_session:
            dates.append(current)
    return tuple(reversed(dates))


def _visible_prices_by_date(
    prices: Sequence[AdjustedDailyPriceObservation],
    asof: datetime,
) -> dict[date, _VisibleAdjustedPrice]:
    visible_prices: dict[date, _VisibleAdjustedPrice] = {}
    seen_dates: set[date] = set()
    for price in prices:
        _validate_price(price)
        if price.price_date in seen_dates:
            raise RealizedVolatilityInputError(
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


def _daily_simple_returns(prices: tuple[Decimal, ...]) -> tuple[Decimal, ...]:
    returns: list[Decimal] = []
    for previous_price, current_price in zip(prices, prices[1:]):
        try:
            daily_return = current_price / previous_price - Decimal("1")
        except (DivisionByZero, InvalidOperation) as exc:
            raise RealizedVolatilityInputError(
                "realized-volatility prices must be valid"
            ) from exc
        if not daily_return.is_finite():
            raise RealizedVolatilityInputError(
                "realized-volatility daily returns must be finite"
            )
        returns.append(daily_return)
    return tuple(returns)


def _annualized_sample_standard_deviation(
    returns: tuple[Decimal, ...],
) -> float:
    if len(returns) < 2:
        raise RealizedVolatilityInputError(
            "realized volatility requires at least two daily returns"
        )

    count = Decimal(len(returns))
    mean = sum(returns, Decimal("0")) / count
    variance = sum((value - mean) ** 2 for value in returns) / (count - Decimal("1"))
    if not variance.is_finite() or variance < 0:
        raise RealizedVolatilityInputError(
            "realized-volatility variance must be finite and non-negative"
        )

    annualized = math.sqrt(float(variance)) * math.sqrt(ANNUALIZATION_SESSIONS)
    if not math.isfinite(annualized):
        raise RealizedVolatilityInputError(
            "realized-volatility value must be finite"
        )
    return annualized


def _validate_price(price: AdjustedDailyPriceObservation) -> None:
    if not isinstance(price.adjusted_close, Decimal):
        raise RealizedVolatilityInputError("adjusted_close must be a Decimal")
    if not price.adjusted_close.is_finite() or price.adjusted_close <= 0:
        raise RealizedVolatilityInputError(
            "adjusted_close must be finite and positive"
        )
    _require_aware(price.available_at, "price.available_at")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise RealizedVolatilityInputError(f"{field_name} must be timezone-aware")
