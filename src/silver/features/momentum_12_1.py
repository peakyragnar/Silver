"""Point-in-time 12-1 momentum from adjusted daily prices."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, DivisionByZero, InvalidOperation
from types import MappingProxyType
from typing import Literal
from zoneinfo import ZoneInfo

from silver.time.trading_calendar import TradingCalendar


FEATURE_NAME = "momentum_12_1"
FEATURE_VERSION = 1
LONG_LOOKBACK_SESSIONS = 252
SKIP_RECENT_SESSIONS = 21
DAILY_PRICE_POLICY_NAME = "daily_price"
DAILY_PRICE_POLICY_VERSION = 1
DAILY_PRICE_TIMEZONE = ZoneInfo("America/New_York")
DAILY_PRICE_AVAILABLE_TIME = time(hour=18)

FeatureStatus = Literal["ok", "insufficient_history", "missing_price"]


class MomentumInputError(ValueError):
    """Raised when inputs cannot produce deterministic point-in-time momentum."""


@dataclass(frozen=True, slots=True)
class NumericFeatureDefinition:
    """Stable feature-definition metadata used before database persistence exists."""

    name: str
    version: int
    kind: Literal["numeric"]
    computation_spec: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class AdjustedDailyPriceObservation:
    """Adjusted close plus the earliest timestamp it may be used."""

    price_date: date
    adjusted_close: Decimal
    available_at: datetime


@dataclass(frozen=True, slots=True)
class MomentumWindow:
    """Trading-session window selected for a 12-1 momentum value."""

    anchor_date: date | None
    start_date: date | None
    end_date: date | None
    start_available_at: datetime | None = None
    end_available_at: datetime | None = None
    missing_price_dates: tuple[date, ...] = ()


@dataclass(frozen=True, slots=True)
class NumericFeatureValue:
    """Typed numeric feature value ready to map to feature_values later."""

    security_id: int
    asof_date: date
    available_at: datetime
    definition: NumericFeatureDefinition
    value: float | None
    status: FeatureStatus
    window: MomentumWindow


@dataclass(frozen=True, slots=True)
class _VisibleAdjustedPrice:
    adjusted_close: Decimal
    effective_available_at: datetime


MOMENTUM_12_1_DEFINITION = NumericFeatureDefinition(
    name=FEATURE_NAME,
    version=FEATURE_VERSION,
    kind="numeric",
    computation_spec=MappingProxyType(
        {
            "adjusted_price_field": "adj_close",
            "return_type": "simple_return",
            "long_lookback_sessions": LONG_LOOKBACK_SESSIONS,
            "skip_recent_sessions": SKIP_RECENT_SESSIONS,
            "daily_price_policy_name": DAILY_PRICE_POLICY_NAME,
            "daily_price_policy_version": DAILY_PRICE_POLICY_VERSION,
        }
    ),
)


def daily_price_available_at(price_date: date) -> datetime:
    """Return daily-price v1 availability: price date at 18:00 America/New_York."""
    return datetime.combine(
        price_date,
        DAILY_PRICE_AVAILABLE_TIME,
        tzinfo=DAILY_PRICE_TIMEZONE,
    )


def compute_momentum_12_1(
    *,
    security_id: int,
    asof: datetime,
    prices: Sequence[AdjustedDailyPriceObservation],
    calendar: TradingCalendar,
) -> NumericFeatureValue:
    """Compute 12-1 momentum using only prices visible at ``asof``.

    The selected value is ``adj_close(t-21) / adj_close(t-252) - 1`` where
    offsets are trading sessions. Price observations are visible only after both
    their source ``available_at`` and the daily-price policy timestamp.
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
            window=MomentumWindow(None, None, None),
        )

    start_date = _session_offset(
        calendar=calendar,
        anchor_date=anchor_date,
        offset=LONG_LOOKBACK_SESSIONS,
    )
    if start_date is None:
        return _null_value(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            status="insufficient_history",
            window=MomentumWindow(anchor_date, None, None),
        )

    end_date = _session_offset(
        calendar=calendar,
        anchor_date=anchor_date,
        offset=SKIP_RECENT_SESSIONS,
    )
    if end_date is None:
        return _null_value(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            status="insufficient_history",
            window=MomentumWindow(anchor_date, start_date, end_date),
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
            status="missing_price",
            window=MomentumWindow(
                anchor_date=anchor_date,
                start_date=start_date,
                end_date=end_date,
                missing_price_dates=missing_dates,
            ),
        )

    start_price = visible_prices[start_date]
    end_price = visible_prices[end_date]
    return NumericFeatureValue(
        security_id=security_id,
        asof_date=asof_date,
        available_at=asof,
        definition=MOMENTUM_12_1_DEFINITION,
        value=_momentum_value(
            start_price=start_price.adjusted_close,
            end_price=end_price.adjusted_close,
        ),
        status="ok",
        window=MomentumWindow(
            anchor_date=anchor_date,
            start_date=start_date,
            end_date=end_date,
            start_available_at=start_price.effective_available_at,
            end_available_at=end_price.effective_available_at,
        ),
    )


def _null_value(
    *,
    security_id: int,
    asof: datetime,
    asof_date: date,
    status: Literal["insufficient_history", "missing_price"],
    window: MomentumWindow,
) -> NumericFeatureValue:
    return NumericFeatureValue(
        security_id=security_id,
        asof_date=asof_date,
        available_at=asof,
        definition=MOMENTUM_12_1_DEFINITION,
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
            raise MomentumInputError(
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


def _momentum_value(*, start_price: Decimal, end_price: Decimal) -> float:
    try:
        value = end_price / start_price - Decimal("1")
    except (DivisionByZero, InvalidOperation) as exc:
        raise MomentumInputError("momentum boundary prices must be valid") from exc
    if not value.is_finite():
        raise MomentumInputError("momentum value must be finite")
    return float(value)


def _validate_price(price: AdjustedDailyPriceObservation) -> None:
    if not isinstance(price.adjusted_close, Decimal):
        raise MomentumInputError("adjusted_close must be a Decimal")
    if not price.adjusted_close.is_finite() or price.adjusted_close <= 0:
        raise MomentumInputError("adjusted_close must be finite and positive")
    _require_aware(price.available_at, "price.available_at")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise MomentumInputError(f"{field_name} must be timezone-aware")
