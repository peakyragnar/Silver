"""Point-in-time average dollar volume from adjusted daily prices and volume."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Literal
from zoneinfo import ZoneInfo

from silver.features.momentum_12_1 import (
    DAILY_PRICE_POLICY_NAME,
    DAILY_PRICE_POLICY_VERSION,
    NumericFeatureDefinition,
    daily_price_available_at,
)
from silver.time.trading_calendar import TradingCalendar


FEATURE_NAME = "avg_dollar_volume_63"
FEATURE_VERSION = 1
LOOKBACK_SESSIONS = 63
DAILY_PRICE_TIMEZONE = ZoneInfo("America/New_York")

DollarVolumeStatus = Literal[
    "ok",
    "insufficient_history",
    "missing_price",
    "missing_volume",
]


class DollarVolumeInputError(ValueError):
    """Raised when inputs cannot produce deterministic point-in-time liquidity."""


@dataclass(frozen=True, slots=True)
class AdjustedPriceVolumeObservation:
    """Adjusted close, volume, and the earliest timestamp they may be used."""

    price_date: date
    adjusted_close: Decimal | None
    volume: int | None
    available_at: datetime


@dataclass(frozen=True, slots=True)
class DollarVolumeWindow:
    """Trading-session window selected for an average dollar volume value."""

    anchor_date: date | None
    start_date: date | None
    end_date: date | None
    start_available_at: datetime | None = None
    end_available_at: datetime | None = None
    observation_count: int = 0
    missing_price_dates: tuple[date, ...] = ()
    missing_volume_dates: tuple[date, ...] = ()


@dataclass(frozen=True, slots=True)
class DollarVolumeFeatureValue:
    """Typed liquidity feature value ready to map to feature_values later."""

    security_id: int
    asof_date: date
    available_at: datetime
    definition: NumericFeatureDefinition
    value: float | None
    status: DollarVolumeStatus
    window: DollarVolumeWindow


@dataclass(frozen=True, slots=True)
class _VisiblePriceVolume:
    adjusted_close: Decimal | None
    volume: int | None
    effective_available_at: datetime


AVG_DOLLAR_VOLUME_63_DEFINITION = NumericFeatureDefinition(
    name=FEATURE_NAME,
    version=FEATURE_VERSION,
    kind="numeric",
    computation_spec=MappingProxyType(
        {
            "adjusted_price_field": "adj_close",
            "volume_field": "volume",
            "aggregation": "arithmetic_mean",
            "lookback_sessions": LOOKBACK_SESSIONS,
            "lookback_includes_anchor": True,
            "daily_price_policy_name": DAILY_PRICE_POLICY_NAME,
            "daily_price_policy_version": DAILY_PRICE_POLICY_VERSION,
        }
    ),
)


def compute_avg_dollar_volume_63(
    *,
    security_id: int,
    asof: datetime,
    observations: Sequence[AdjustedPriceVolumeObservation],
    calendar: TradingCalendar,
) -> DollarVolumeFeatureValue:
    """Compute 63-session average dollar volume using only rows visible at ``asof``.

    The value is the arithmetic mean of ``adj_close * volume`` over the latest
    63 trading sessions whose daily price rows are visible under the daily-price
    availability policy.
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
            window=DollarVolumeWindow(None, None, None),
        )

    window_dates = _rolling_window_sessions(
        calendar=calendar,
        anchor_date=anchor_date,
        lookback_sessions=LOOKBACK_SESSIONS,
    )
    if window_dates is None:
        return _null_value(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            status="insufficient_history",
            window=DollarVolumeWindow(anchor_date, None, None),
        )

    visible_observations = _visible_observations_by_date(observations, asof)
    missing_price_dates = tuple(
        price_date
        for price_date in window_dates
        if price_date not in visible_observations
        or visible_observations[price_date].adjusted_close is None
    )
    if missing_price_dates:
        return _null_value(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            status="missing_price",
            window=DollarVolumeWindow(
                anchor_date=anchor_date,
                start_date=window_dates[0],
                end_date=window_dates[-1],
                missing_price_dates=missing_price_dates,
            ),
        )

    missing_volume_dates = tuple(
        price_date
        for price_date in window_dates
        if visible_observations[price_date].volume is None
    )
    if missing_volume_dates:
        return _null_value(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            status="missing_volume",
            window=DollarVolumeWindow(
                anchor_date=anchor_date,
                start_date=window_dates[0],
                end_date=window_dates[-1],
                missing_volume_dates=missing_volume_dates,
            ),
        )

    selected_observations = tuple(
        visible_observations[price_date] for price_date in window_dates
    )
    return DollarVolumeFeatureValue(
        security_id=security_id,
        asof_date=asof_date,
        available_at=asof,
        definition=AVG_DOLLAR_VOLUME_63_DEFINITION,
        value=_average_dollar_volume(selected_observations),
        status="ok",
        window=DollarVolumeWindow(
            anchor_date=anchor_date,
            start_date=window_dates[0],
            end_date=window_dates[-1],
            start_available_at=selected_observations[0].effective_available_at,
            end_available_at=selected_observations[-1].effective_available_at,
            observation_count=len(selected_observations),
        ),
    )


def _null_value(
    *,
    security_id: int,
    asof: datetime,
    asof_date: date,
    status: Literal["insufficient_history", "missing_price", "missing_volume"],
    window: DollarVolumeWindow,
) -> DollarVolumeFeatureValue:
    return DollarVolumeFeatureValue(
        security_id=security_id,
        asof_date=asof_date,
        available_at=asof,
        definition=AVG_DOLLAR_VOLUME_63_DEFINITION,
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


def _rolling_window_sessions(
    *,
    calendar: TradingCalendar,
    anchor_date: date,
    lookback_sessions: int,
) -> tuple[date, ...] | None:
    first_calendar_date = calendar.rows[0].date
    current = anchor_date
    window_dates: list[date] = []
    while len(window_dates) < lookback_sessions:
        if current < first_calendar_date:
            return None

        row = calendar.row_for(current)
        if row.is_session:
            window_dates.append(current)
        current -= timedelta(days=1)

    return tuple(reversed(window_dates))


def _visible_observations_by_date(
    observations: Sequence[AdjustedPriceVolumeObservation],
    asof: datetime,
) -> dict[date, _VisiblePriceVolume]:
    visible_observations: dict[date, _VisiblePriceVolume] = {}
    seen_dates: set[date] = set()
    for observation in observations:
        _validate_observation(observation)
        if observation.price_date in seen_dates:
            raise DollarVolumeInputError(
                "duplicate adjusted daily price/volume observation for "
                f"{observation.price_date.isoformat()}"
            )
        seen_dates.add(observation.price_date)

        effective_available_at = _effective_price_available_at(observation)
        if effective_available_at <= asof:
            visible_observations[observation.price_date] = _VisiblePriceVolume(
                adjusted_close=observation.adjusted_close,
                volume=observation.volume,
                effective_available_at=effective_available_at,
            )
    return visible_observations


def _effective_price_available_at(
    observation: AdjustedPriceVolumeObservation,
) -> datetime:
    policy_available_at = daily_price_available_at(observation.price_date)
    if observation.available_at >= policy_available_at:
        return observation.available_at
    return policy_available_at


def _average_dollar_volume(
    observations: Sequence[_VisiblePriceVolume],
) -> float:
    if not observations:
        raise DollarVolumeInputError("dollar-volume window must not be empty")

    total = Decimal("0")
    for observation in observations:
        if observation.adjusted_close is None or observation.volume is None:
            raise DollarVolumeInputError(
                "dollar-volume observations must have price and volume"
            )
        total += observation.adjusted_close * Decimal(observation.volume)

    try:
        value = total / Decimal(len(observations))
    except InvalidOperation as exc:
        raise DollarVolumeInputError("dollar-volume value must be valid") from exc
    if not value.is_finite():
        raise DollarVolumeInputError("dollar-volume value must be finite")
    return float(value)


def _validate_observation(observation: AdjustedPriceVolumeObservation) -> None:
    if not isinstance(observation, AdjustedPriceVolumeObservation):
        raise DollarVolumeInputError(
            "observations must be AdjustedPriceVolumeObservation instances"
        )
    if observation.adjusted_close is not None:
        if not isinstance(observation.adjusted_close, Decimal):
            raise DollarVolumeInputError("adjusted_close must be a Decimal")
        if (
            not observation.adjusted_close.is_finite()
            or observation.adjusted_close <= 0
        ):
            raise DollarVolumeInputError(
                "adjusted_close must be finite and positive"
            )
    if observation.volume is not None:
        if isinstance(observation.volume, bool) or not isinstance(
            observation.volume,
            int,
        ):
            raise DollarVolumeInputError("volume must be an integer")
        if observation.volume < 0:
            raise DollarVolumeInputError("volume must be non-negative")
    _require_aware(observation.available_at, "observation.available_at")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise DollarVolumeInputError(f"{field_name} must be timezone-aware")
