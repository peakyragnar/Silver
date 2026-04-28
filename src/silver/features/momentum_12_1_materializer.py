"""Materialize 12-1 momentum into the Silver feature store."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Protocol

from silver.features.momentum_12_1 import (
    DAILY_PRICE_POLICY_NAME,
    DAILY_PRICE_POLICY_VERSION,
    MOMENTUM_12_1_DEFINITION,
    AdjustedDailyPriceObservation,
    NumericFeatureDefinition,
    compute_momentum_12_1,
    daily_price_available_at,
)
from silver.features.repository import (
    AvailableAtPolicyRecord,
    FeatureDefinitionRecord,
    FeatureStoreError,
    FeatureValueWrite,
    UniverseMembershipRecord,
)
from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


class MomentumFeatureRepository(Protocol):
    def ensure_feature_definition(
        self,
        definition: NumericFeatureDefinition,
        *,
        notes: str | None = None,
    ) -> FeatureDefinitionRecord:
        ...

    def load_available_at_policy(
        self,
        *,
        name: str,
        version: int,
    ) -> AvailableAtPolicyRecord:
        ...

    def load_universe_memberships(
        self,
        *,
        universe_name: str,
        start_date: date | None,
        end_date: date | None,
    ) -> tuple[UniverseMembershipRecord, ...]:
        ...

    def load_trading_calendar(
        self,
        *,
        end_date: date | None,
    ) -> tuple[TradingCalendarRow, ...]:
        ...

    def load_adjusted_prices(
        self,
        *,
        security_ids: Sequence[int],
        end_date: date | None,
        available_at_policy_id: int,
    ) -> tuple[tuple[int, AdjustedDailyPriceObservation], ...]:
        ...

    def write_feature_values(
        self,
        values: Sequence[FeatureValueWrite],
    ) -> object:
        ...


@dataclass(frozen=True, slots=True)
class MomentumMaterializationSummary:
    feature_definition_id: int
    universe_name: str
    requested_start_date: date | None
    requested_end_date: date | None
    materialized_start_date: date | None
    materialized_end_date: date | None
    securities_seen: int
    eligible_security_dates: int
    values_written: int
    skipped_insufficient_history: int
    skipped_missing_price: int

    @property
    def skipped_total(self) -> int:
        return self.skipped_insufficient_history + self.skipped_missing_price


def materialize_momentum_12_1(
    repository: MomentumFeatureRepository,
    *,
    universe_name: str,
    start_date: date | None,
    end_date: date | None,
    computed_by_run_id: int,
    dry_run: bool = False,
) -> MomentumMaterializationSummary:
    """Compute and persist eligible 12-1 momentum feature values."""
    _validate_date_bounds(start_date=start_date, end_date=end_date)
    if computed_by_run_id <= 0:
        raise FeatureStoreError("computed_by_run_id must be a positive integer")

    definition = repository.ensure_feature_definition(
        MOMENTUM_12_1_DEFINITION,
        notes="Deterministic 12-1 simple-return momentum from adjusted closes.",
    )
    policy = repository.load_available_at_policy(
        name=DAILY_PRICE_POLICY_NAME,
        version=DAILY_PRICE_POLICY_VERSION,
    )
    memberships = repository.load_universe_memberships(
        universe_name=universe_name,
        start_date=start_date,
        end_date=end_date,
    )
    if not memberships:
        raise FeatureStoreError(f"universe {universe_name} has no eligible members")

    security_ids = tuple(sorted({membership.security_id for membership in memberships}))
    prices = repository.load_adjusted_prices(
        security_ids=security_ids,
        end_date=end_date,
        available_at_policy_id=policy.id,
    )
    prices_by_security = _prices_by_security(prices)
    price_dates = [price.price_date for _, price in prices]
    effective_start_date = start_date or (min(price_dates) if price_dates else None)
    effective_end_date = end_date or (max(price_dates) if price_dates else None)

    calendar_rows = repository.load_trading_calendar(end_date=effective_end_date)
    calendar = TradingCalendar(calendar_rows)
    candidate_dates = _candidate_asof_dates(
        calendar_rows=calendar.rows,
        start_date=effective_start_date,
        end_date=effective_end_date,
    )

    writes: list[FeatureValueWrite] = []
    skipped: Counter[str] = Counter()
    eligible_security_dates = 0
    for asof_date in candidate_dates:
        asof = daily_price_available_at(asof_date).astimezone(timezone.utc)
        for membership in memberships:
            if not membership.is_active_on(asof_date):
                continue
            eligible_security_dates += 1
            result = compute_momentum_12_1(
                security_id=membership.security_id,
                asof=asof,
                prices=prices_by_security.get(membership.security_id, ()),
                calendar=calendar,
            )
            if result.status != "ok" or result.value is None:
                skipped[result.status] += 1
                continue

            writes.append(
                FeatureValueWrite(
                    security_id=result.security_id,
                    asof_date=result.asof_date,
                    feature_definition_id=definition.id,
                    value=result.value,
                    available_at=result.available_at,
                    available_at_policy_id=policy.id,
                    computed_by_run_id=computed_by_run_id,
                    source_metadata=_source_metadata(
                        universe_name=universe_name,
                        policy=policy,
                        result_available_at=result.available_at,
                        window={
                            "anchor_date": result.window.anchor_date,
                            "start_date": result.window.start_date,
                            "end_date": result.window.end_date,
                            "start_available_at": result.window.start_available_at,
                            "end_available_at": result.window.end_available_at,
                        },
                    ),
                )
            )

    if not dry_run:
        repository.write_feature_values(writes)

    return MomentumMaterializationSummary(
        feature_definition_id=definition.id,
        universe_name=universe_name,
        requested_start_date=start_date,
        requested_end_date=end_date,
        materialized_start_date=candidate_dates[0] if candidate_dates else None,
        materialized_end_date=candidate_dates[-1] if candidate_dates else None,
        securities_seen=len(security_ids),
        eligible_security_dates=eligible_security_dates,
        values_written=len(writes),
        skipped_insufficient_history=skipped["insufficient_history"],
        skipped_missing_price=skipped["missing_price"],
    )


def _prices_by_security(
    rows: Sequence[tuple[int, AdjustedDailyPriceObservation]],
) -> dict[int, tuple[AdjustedDailyPriceObservation, ...]]:
    grouped: defaultdict[int, list[AdjustedDailyPriceObservation]] = defaultdict(list)
    for security_id, price in rows:
        grouped[security_id].append(price)
    return {
        security_id: tuple(sorted(prices, key=lambda price: price.price_date))
        for security_id, prices in grouped.items()
    }


def _candidate_asof_dates(
    *,
    calendar_rows: Sequence[TradingCalendarRow],
    start_date: date | None,
    end_date: date | None,
) -> tuple[date, ...]:
    return tuple(
        row.date
        for row in calendar_rows
        if row.is_session
        and (start_date is None or row.date >= start_date)
        and (end_date is None or row.date <= end_date)
    )


def _source_metadata(
    *,
    universe_name: str,
    policy: AvailableAtPolicyRecord,
    result_available_at: datetime,
    window: Mapping[str, object],
) -> dict[str, object]:
    return {
        "source": "silver.features.momentum_12_1",
        "universe_name": universe_name,
        "available_at": result_available_at.isoformat(),
        "daily_price_policy": {"name": policy.name, "version": policy.version},
        "window": {
            key: _metadata_value(value)
            for key, value in window.items()
            if value is not None
        },
    }


def _metadata_value(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _validate_date_bounds(*, start_date: date | None, end_date: date | None) -> None:
    if start_date is not None and not isinstance(start_date, date):
        raise FeatureStoreError("start_date must be a date")
    if end_date is not None and not isinstance(end_date, date):
        raise FeatureStoreError("end_date must be a date")
    if start_date is not None and end_date is not None and end_date < start_date:
        raise FeatureStoreError("end_date must be on or after start_date")
