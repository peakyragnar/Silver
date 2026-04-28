"""Pure forward-label materialization from normalized price observations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from silver.labels.forward_returns import (
    ForwardReturnLabel,
    SkippedForwardReturnLabel,
    calculate_forward_return_labels,
)
from silver.labels.repository import (
    ForwardLabelPersistenceError,
    ForwardLabelPriceObservation,
    ForwardLabelRecord,
)
from silver.time.trading_calendar import CANONICAL_HORIZONS, TradingCalendar


@dataclass(frozen=True, slots=True)
class ForwardLabelMaterializationResult:
    """Pure materialization output ready for repository writes."""

    records: tuple[ForwardLabelRecord, ...]
    skipped: tuple[SkippedForwardReturnLabel, ...]


def build_forward_label_records(
    *,
    prices: Sequence[ForwardLabelPriceObservation],
    calendar: TradingCalendar,
    label_dates_by_security: Mapping[int, Sequence[date]],
    computed_by_run_id: int,
    horizons: Sequence[int] = CANONICAL_HORIZONS,
    label_version: int = 1,
    benchmark_prices: Sequence[ForwardLabelPriceObservation] | None = None,
) -> ForwardLabelMaterializationResult:
    """Calculate database-ready label records from normalized prices.

    The existing calculator owns forward-return and trading-day arithmetic. This
    materializer adds database lineage and keeps label ``available_at`` no earlier
    than the target session close, target normalized price row, and any configured
    benchmark price rows used for benchmark-relative metadata or returns.
    """
    normalized_prices = _validated_prices(prices)
    benchmark = _benchmark_observations(benchmark_prices)
    normalized_label_dates = _label_dates(label_dates_by_security)
    if isinstance(computed_by_run_id, bool) or not isinstance(computed_by_run_id, int):
        raise ForwardLabelPersistenceError("computed_by_run_id must be an integer")
    if computed_by_run_id <= 0:
        raise ForwardLabelPersistenceError("computed_by_run_id must be positive")
    if isinstance(label_version, bool) or not isinstance(label_version, int):
        raise ForwardLabelPersistenceError("label_version must be an integer")
    if label_version <= 0:
        raise ForwardLabelPersistenceError("label_version must be positive")

    observations = _index_observations(normalized_prices)
    benchmark_price_rows = (
        tuple(observation.row for observation in benchmark.rows)
        if benchmark is not None
        else None
    )
    records: list[ForwardLabelRecord] = []
    skipped: list[SkippedForwardReturnLabel] = []

    for security_id in sorted(normalized_label_dates):
        security_observations = observations.get(security_id)
        if security_observations is None:
            continue
        ticker = security_observations.ticker
        batch = calculate_forward_return_labels(
            prices=tuple(observation.row for observation in security_observations.rows),
            calendar=calendar,
            asof_dates=normalized_label_dates[security_id],
            horizons=horizons,
            benchmark_prices=benchmark_price_rows,
            benchmark_ticker=benchmark.ticker if benchmark is not None else None,
        )
        skipped.extend(batch.skipped)
        for label in batch.labels:
            start = security_observations.by_date[label.asof_date]
            target = security_observations.by_date[label.target_date]
            records.append(
                _record_from_label(
                    security_id=security_id,
                    ticker=ticker,
                    label=label,
                    start=start,
                    target=target,
                    computed_by_run_id=computed_by_run_id,
                    label_version=label_version,
                    benchmark=benchmark,
                )
            )

    return ForwardLabelMaterializationResult(
        records=tuple(records),
        skipped=tuple(skipped),
    )


@dataclass(frozen=True, slots=True)
class _SecurityObservations:
    security_id: int
    ticker: str
    rows: tuple[ForwardLabelPriceObservation, ...]
    by_date: Mapping[date, ForwardLabelPriceObservation]


@dataclass(frozen=True, slots=True)
class _BenchmarkLabelFields:
    benchmark_security_id: int
    realized_excess_return: Decimal | None
    metadata: Mapping[str, object]
    availability_observations: tuple[ForwardLabelPriceObservation, ...]


def _record_from_label(
    *,
    security_id: int,
    ticker: str,
    label: ForwardReturnLabel,
    start: ForwardLabelPriceObservation,
    target: ForwardLabelPriceObservation,
    computed_by_run_id: int,
    label_version: int,
    benchmark: _SecurityObservations | None,
) -> ForwardLabelRecord:
    available_at = _max_datetime(label.available_at, target.available_at)
    available_at_policy_id = target.available_at_policy_id
    metadata = {
        "calculator": "silver.labels.forward_returns.calculate_forward_return_labels",
        "security": ticker,
        "start_price_available_at": start.available_at.isoformat(),
        "start_price_available_at_policy_id": start.available_at_policy_id,
        "target_price_available_at": target.available_at.isoformat(),
        "target_price_available_at_policy_id": target.available_at_policy_id,
    }
    benchmark_security_id: int | None = None
    realized_excess_return: Decimal | None = None
    if benchmark is not None:
        benchmark_fields = _benchmark_label_fields(benchmark, label)
        benchmark_security_id = benchmark_fields.benchmark_security_id
        realized_excess_return = benchmark_fields.realized_excess_return
        metadata["benchmark"] = benchmark_fields.metadata
        available_at, available_at_policy_id = _max_observation_availability(
            available_at,
            available_at_policy_id,
            benchmark_fields.availability_observations,
        )

    return ForwardLabelRecord(
        security_id=security_id,
        label_date=label.asof_date,
        horizon_days=label.horizon_days,
        horizon_date=label.target_date,
        horizon_close_at=label.available_at,
        label_version=label_version,
        start_adj_close=label.asof_adj_close,
        end_adj_close=label.target_adj_close,
        realized_raw_return=label.forward_return,
        benchmark_security_id=benchmark_security_id,
        realized_excess_return=realized_excess_return,
        available_at=available_at,
        available_at_policy_id=available_at_policy_id,
        computed_by_run_id=computed_by_run_id,
        metadata=metadata,
    )


def _benchmark_label_fields(
    benchmark: _SecurityObservations,
    label: ForwardReturnLabel,
) -> _BenchmarkLabelFields:
    start = benchmark.by_date.get(label.asof_date)
    target = benchmark.by_date.get(label.target_date)
    availability_observations = tuple(
        observation for observation in (start, target) if observation is not None
    )
    metadata: dict[str, object] = {
        "ticker": benchmark.ticker,
        "security_id": benchmark.security_id,
        "asof_date": label.asof_date.isoformat(),
        "target_date": label.target_date.isoformat(),
    }

    if start is None:
        metadata.update(
            {
                "status": "missing_asof_price",
                "missing_price_date": label.asof_date.isoformat(),
            }
        )
        return _BenchmarkLabelFields(
            benchmark_security_id=benchmark.security_id,
            realized_excess_return=None,
            metadata=metadata,
            availability_observations=availability_observations,
        )

    if target is None:
        metadata.update(
            {
                "status": "missing_target_price",
                "missing_price_date": label.target_date.isoformat(),
            }
        )
        return _BenchmarkLabelFields(
            benchmark_security_id=benchmark.security_id,
            realized_excess_return=None,
            metadata=metadata,
            availability_observations=availability_observations,
        )

    if label.benchmark_forward_return is None or label.excess_return is None:
        metadata["status"] = "unavailable_return"
        return _BenchmarkLabelFields(
            benchmark_security_id=benchmark.security_id,
            realized_excess_return=None,
            metadata=metadata,
            availability_observations=availability_observations,
        )

    metadata.update(
        {
            "status": "covered",
            "start_price_available_at": start.available_at.isoformat(),
            "start_price_available_at_policy_id": start.available_at_policy_id,
            "target_price_available_at": target.available_at.isoformat(),
            "target_price_available_at_policy_id": target.available_at_policy_id,
            "benchmark_forward_return": str(label.benchmark_forward_return),
        }
    )
    return _BenchmarkLabelFields(
        benchmark_security_id=benchmark.security_id,
        realized_excess_return=label.excess_return,
        metadata=metadata,
        availability_observations=availability_observations,
    )


def _validated_prices(
    prices: Sequence[ForwardLabelPriceObservation],
) -> tuple[ForwardLabelPriceObservation, ...]:
    if isinstance(prices, (str, bytes)) or not isinstance(prices, Sequence):
        raise ForwardLabelPersistenceError(
            "prices must be a sequence of ForwardLabelPriceObservation"
        )
    normalized = tuple(prices)
    for index, observation in enumerate(normalized, start=1):
        if not isinstance(observation, ForwardLabelPriceObservation):
            raise ForwardLabelPersistenceError(
                f"prices[{index}] must be a ForwardLabelPriceObservation"
            )
        _validate_observation(observation)
    return normalized


def _benchmark_observations(
    benchmark_prices: Sequence[ForwardLabelPriceObservation] | None,
) -> _SecurityObservations | None:
    if benchmark_prices is None:
        return None
    normalized_prices = _validated_prices(benchmark_prices)
    if not normalized_prices:
        raise ForwardLabelPersistenceError(
            "benchmark_prices must contain at least one observation when provided"
        )
    indexed = _index_observations(normalized_prices)
    if len(indexed) != 1:
        raise ForwardLabelPersistenceError(
            "benchmark_prices must contain observations for exactly one security"
        )
    return next(iter(indexed.values()))


def _validate_observation(observation: ForwardLabelPriceObservation) -> None:
    if (
        isinstance(observation.security_id, bool)
        or not isinstance(observation.security_id, int)
        or observation.security_id <= 0
    ):
        raise ForwardLabelPersistenceError("security_id must be a positive integer")
    if not isinstance(observation.available_at, datetime):
        raise ForwardLabelPersistenceError("price available_at must be a datetime")
    if (
        observation.available_at.tzinfo is None
        or observation.available_at.utcoffset() is None
    ):
        raise ForwardLabelPersistenceError("price available_at must be timezone-aware")
    if (
        isinstance(observation.available_at_policy_id, bool)
        or not isinstance(observation.available_at_policy_id, int)
        or observation.available_at_policy_id <= 0
    ):
        raise ForwardLabelPersistenceError(
            "available_at_policy_id must be a positive integer"
        )
    if not observation.row.ticker.strip():
        raise ForwardLabelPersistenceError("price ticker must be non-empty")
    for field_name in ("open", "high", "low", "close", "adj_close"):
        value = getattr(observation.row, field_name)
        if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
            raise ForwardLabelPersistenceError(
                f"price {field_name} must be a positive finite Decimal"
            )


def _label_dates(
    label_dates_by_security: Mapping[int, Sequence[date]],
) -> dict[int, tuple[date, ...]]:
    if not isinstance(label_dates_by_security, Mapping):
        raise ForwardLabelPersistenceError("label_dates_by_security must be a mapping")
    normalized: dict[int, tuple[date, ...]] = {}
    for security_id, label_dates in label_dates_by_security.items():
        if isinstance(security_id, bool) or not isinstance(security_id, int):
            raise ForwardLabelPersistenceError("label-date security_id must be an integer")
        if security_id <= 0:
            raise ForwardLabelPersistenceError("label-date security_id must be positive")
        if isinstance(label_dates, (str, bytes)) or not isinstance(
            label_dates,
            Sequence,
        ):
            raise ForwardLabelPersistenceError("label dates must be a sequence")
        normalized_dates = tuple(sorted(set(label_dates)))
        for label_date in normalized_dates:
            if isinstance(label_date, datetime) or not isinstance(label_date, date):
                raise ForwardLabelPersistenceError("label dates must contain date values")
        normalized[security_id] = normalized_dates
    return normalized


def _index_observations(
    prices: Sequence[ForwardLabelPriceObservation],
) -> dict[int, _SecurityObservations]:
    grouped: dict[int, list[ForwardLabelPriceObservation]] = {}
    for observation in prices:
        grouped.setdefault(observation.security_id, []).append(observation)

    indexed: dict[int, _SecurityObservations] = {}
    for security_id, rows in grouped.items():
        sorted_rows = tuple(sorted(rows, key=lambda observation: observation.row.date))
        tickers = {observation.row.ticker.strip().upper() for observation in sorted_rows}
        if len(tickers) != 1:
            raise ForwardLabelPersistenceError(
                f"security_id {security_id} has multiple tickers in price inputs"
            )

        by_date: dict[date, ForwardLabelPriceObservation] = {}
        for observation in sorted_rows:
            price_date = observation.row.date
            if price_date in by_date:
                raise ForwardLabelPersistenceError(
                    f"duplicate price input for security_id {security_id} "
                    f"on {price_date.isoformat()}"
                )
            by_date[price_date] = observation

        indexed[security_id] = _SecurityObservations(
            security_id=security_id,
            ticker=next(iter(tickers)),
            rows=sorted_rows,
            by_date=by_date,
        )
    return indexed


def _max_datetime(first: datetime, second: datetime) -> datetime:
    return second if second > first else first


def _max_observation_availability(
    available_at: datetime,
    available_at_policy_id: int,
    observations: Sequence[ForwardLabelPriceObservation],
) -> tuple[datetime, int]:
    for observation in observations:
        if observation.available_at > available_at:
            available_at = observation.available_at
            available_at_policy_id = observation.available_at_policy_id
    return available_at, available_at_policy_id
