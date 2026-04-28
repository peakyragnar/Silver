"""Deterministic forward-return labels for Phase 1 trading horizons."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum

from silver.prices.daily import DailyPriceRow
from silver.time.trading_calendar import (
    CANONICAL_HORIZONS,
    MissingTradingCalendarRowsError,
    NonTradingSessionError,
    TradingCalendar,
)


class ForwardReturnLabelInputError(ValueError):
    """Raised when label calculation inputs are ambiguous or unsupported."""


class SkipReason(str, Enum):
    """Reasons a requested label could not be produced."""

    MISSING_CALENDAR_ROW = "missing_calendar_row"
    NON_SESSION_ASOF = "non_session_asof"
    MISSING_ASOF_PRICE = "missing_asof_price"
    INVALID_ASOF_PRICE = "invalid_asof_price"
    INSUFFICIENT_CALENDAR_HISTORY = "insufficient_calendar_history"
    MISSING_TARGET_PRICE = "missing_target_price"
    INVALID_TARGET_PRICE = "invalid_target_price"
    MISSING_TARGET_SESSION_CLOSE = "missing_target_session_close"


@dataclass(frozen=True, slots=True)
class ForwardReturnLabel:
    """Realized forward return for one security, as-of date, and horizon."""

    security: str
    asof_date: date
    horizon_days: int
    target_date: date
    available_at: datetime
    asof_adj_close: Decimal
    target_adj_close: Decimal
    forward_return: Decimal
    benchmark_forward_return: Decimal | None = None
    excess_return: Decimal | None = None


@dataclass(frozen=True, slots=True)
class SkippedForwardReturnLabel:
    """Explicit record for a requested label that could not be calculated."""

    security: str
    asof_date: date
    horizon_days: int
    reason: SkipReason
    target_date: date | None = None
    message: str = ""


@dataclass(frozen=True, slots=True)
class ForwardReturnLabelBatch:
    """Deterministic label output plus explicit skipped-label records."""

    labels: tuple[ForwardReturnLabel, ...]
    skipped: tuple[SkippedForwardReturnLabel, ...]


def calculate_forward_return_labels(
    *,
    prices: Sequence[DailyPriceRow],
    calendar: TradingCalendar,
    asof_dates: Sequence[date] | None = None,
    horizons: Sequence[int] = CANONICAL_HORIZONS,
    benchmark_prices: Sequence[DailyPriceRow] | None = None,
    benchmark_ticker: str | None = None,
) -> ForwardReturnLabelBatch:
    """Calculate raw and optional benchmark-relative forward-return labels.

    Labels use adjusted closes and trading-session offsets. A label is emitted
    only after the as-of session, target session, and both prices are known.
    Missing prerequisites are returned as explicit skipped records.
    """
    normalized_horizons = _normalize_horizons(horizons)
    prices_by_security = _index_prices(prices, input_name="prices")
    benchmark_by_date = _benchmark_prices_by_date(benchmark_prices, benchmark_ticker)

    labels: list[ForwardReturnLabel] = []
    skipped: list[SkippedForwardReturnLabel] = []
    for security in sorted(prices_by_security):
        security_prices = prices_by_security[security]
        requested_asof_dates = (
            sorted(set(asof_dates))
            if asof_dates is not None
            else sorted(security_prices)
        )
        for asof_date in requested_asof_dates:
            for horizon_days in normalized_horizons:
                label = _calculate_one_label(
                    security=security,
                    security_prices=security_prices,
                    calendar=calendar,
                    asof_date=asof_date,
                    horizon_days=horizon_days,
                    benchmark_prices=benchmark_by_date,
                )
                if isinstance(label, ForwardReturnLabel):
                    labels.append(label)
                else:
                    skipped.append(label)

    return ForwardReturnLabelBatch(labels=tuple(labels), skipped=tuple(skipped))


def _calculate_one_label(
    *,
    security: str,
    security_prices: dict[date, DailyPriceRow],
    calendar: TradingCalendar,
    asof_date: date,
    horizon_days: int,
    benchmark_prices: dict[date, DailyPriceRow] | None,
) -> ForwardReturnLabel | SkippedForwardReturnLabel:
    try:
        asof_row = calendar.row_for(asof_date)
    except MissingTradingCalendarRowsError as exc:
        return _skip(
            security,
            asof_date,
            horizon_days,
            SkipReason.MISSING_CALENDAR_ROW,
            message=str(exc),
        )
    if not asof_row.is_session:
        return _skip(security, asof_date, horizon_days, SkipReason.NON_SESSION_ASOF)

    asof_price = security_prices.get(asof_date)
    if asof_price is None:
        return _skip(security, asof_date, horizon_days, SkipReason.MISSING_ASOF_PRICE)
    if asof_price.adj_close <= 0:
        return _skip(security, asof_date, horizon_days, SkipReason.INVALID_ASOF_PRICE)

    try:
        target_date = calendar.advance_trading_days(asof_date, horizon_days)
    except NonTradingSessionError as exc:
        return _skip(
            security,
            asof_date,
            horizon_days,
            SkipReason.NON_SESSION_ASOF,
            message=str(exc),
        )
    except MissingTradingCalendarRowsError as exc:
        return _skip(
            security,
            asof_date,
            horizon_days,
            SkipReason.INSUFFICIENT_CALENDAR_HISTORY,
            message=str(exc),
        )

    target_price = security_prices.get(target_date)
    if target_price is None:
        return _skip(
            security,
            asof_date,
            horizon_days,
            SkipReason.MISSING_TARGET_PRICE,
            target_date=target_date,
        )
    if target_price.adj_close <= 0:
        return _skip(
            security,
            asof_date,
            horizon_days,
            SkipReason.INVALID_TARGET_PRICE,
            target_date=target_date,
        )

    target_row = calendar.row_for(target_date)
    if target_row.session_close is None:
        return _skip(
            security,
            asof_date,
            horizon_days,
            SkipReason.MISSING_TARGET_SESSION_CLOSE,
            target_date=target_date,
        )

    forward_return = _forward_return(asof_price.adj_close, target_price.adj_close)
    benchmark_forward_return = _benchmark_forward_return(
        benchmark_prices,
        asof_date,
        target_date,
    )
    excess_return = (
        forward_return - benchmark_forward_return
        if benchmark_forward_return is not None
        else None
    )

    return ForwardReturnLabel(
        security=security,
        asof_date=asof_date,
        horizon_days=horizon_days,
        target_date=target_date,
        available_at=target_row.session_close,
        asof_adj_close=asof_price.adj_close,
        target_adj_close=target_price.adj_close,
        forward_return=forward_return,
        benchmark_forward_return=benchmark_forward_return,
        excess_return=excess_return,
    )


def _normalize_horizons(horizons: Sequence[int]) -> tuple[int, ...]:
    normalized = tuple(sorted(set(horizons)))
    if not normalized:
        raise ForwardReturnLabelInputError("at least one horizon is required")

    invalid = [horizon for horizon in normalized if horizon not in CANONICAL_HORIZONS]
    if invalid:
        allowed = ", ".join(str(horizon) for horizon in CANONICAL_HORIZONS)
        got = ", ".join(str(horizon) for horizon in invalid)
        raise ForwardReturnLabelInputError(
            f"horizon_days must be one of {allowed}; got {got}"
        )
    return normalized


def _index_prices(
    prices: Sequence[DailyPriceRow],
    *,
    input_name: str,
) -> dict[str, dict[date, DailyPriceRow]]:
    indexed: dict[str, dict[date, DailyPriceRow]] = {}
    for row in prices:
        security_prices = indexed.setdefault(row.ticker, {})
        if row.date in security_prices:
            raise ForwardReturnLabelInputError(
                f"duplicate {input_name} row for {row.ticker} on {row.date.isoformat()}"
            )
        security_prices[row.date] = row
    return indexed


def _benchmark_prices_by_date(
    benchmark_prices: Sequence[DailyPriceRow] | None,
    benchmark_ticker: str | None,
) -> dict[date, DailyPriceRow] | None:
    if benchmark_prices is None:
        return None

    indexed = _index_prices(benchmark_prices, input_name="benchmark_prices")
    if not indexed:
        return {}

    if benchmark_ticker is None:
        benchmark_tickers = sorted(indexed)
        if len(benchmark_tickers) != 1:
            raise ForwardReturnLabelInputError(
                "benchmark_ticker is required when benchmark_prices contains "
                "multiple securities"
            )
        benchmark_ticker = benchmark_tickers[0]

    return indexed.get(benchmark_ticker, {})


def _benchmark_forward_return(
    benchmark_prices: dict[date, DailyPriceRow] | None,
    asof_date: date,
    target_date: date,
) -> Decimal | None:
    if benchmark_prices is None:
        return None

    asof_price = benchmark_prices.get(asof_date)
    target_price = benchmark_prices.get(target_date)
    if asof_price is None or target_price is None:
        return None
    if asof_price.adj_close <= 0 or target_price.adj_close <= 0:
        return None
    return _forward_return(asof_price.adj_close, target_price.adj_close)


def _forward_return(asof_adj_close: Decimal, target_adj_close: Decimal) -> Decimal:
    return (target_adj_close / asof_adj_close) - Decimal("1")


def _skip(
    security: str,
    asof_date: date,
    horizon_days: int,
    reason: SkipReason,
    *,
    target_date: date | None = None,
    message: str = "",
) -> SkippedForwardReturnLabel:
    return SkippedForwardReturnLabel(
        security=security,
        asof_date=asof_date,
        horizon_days=horizon_days,
        reason=reason,
        target_date=target_date,
        message=message,
    )
