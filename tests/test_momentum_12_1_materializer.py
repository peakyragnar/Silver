from __future__ import annotations

import subprocess
import sys
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from silver.features import MOMENTUM_12_1_DEFINITION
from silver.features.momentum_12_1 import (
    AdjustedDailyPriceObservation,
    daily_price_available_at,
)
from silver.features.momentum_12_1_materializer import materialize_momentum_12_1
from silver.features.repository import (
    AvailableAtPolicyRecord,
    FeatureDefinitionRecord,
    FeatureValueWrite,
    UniverseMembershipRecord,
    feature_definition_hash,
)
from silver.time.trading_calendar import TradingCalendarRow


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "materialize_momentum_12_1.py"
SECURITY_ID = 101


def test_materialize_momentum_writes_one_idempotent_value_for_eligible_pair() -> None:
    calendar_rows, sessions = _calendar_rows(date(2023, 1, 3), session_count=253)
    asof_date = sessions[-1]
    start_date = sessions[-253]
    end_date = sessions[-22]
    repository = FakeMomentumRepository(
        calendar_rows=calendar_rows,
        prices={
            SECURITY_ID: [
                _price(start_date, "100.00"),
                _price(end_date, "125.00"),
            ]
        },
    )

    first = materialize_momentum_12_1(
        repository,
        universe_name="falsifier_seed",
        start_date=asof_date,
        end_date=asof_date,
        computed_by_run_id=77,
    )
    second = materialize_momentum_12_1(
        repository,
        universe_name="falsifier_seed",
        start_date=asof_date,
        end_date=asof_date,
        computed_by_run_id=77,
    )

    assert first.values_written == 1
    assert first.skipped_total == 0
    assert second.values_written == 1
    assert len(repository.feature_values) == 1
    [stored] = repository.feature_values.values()
    assert stored.value == 0.25
    assert stored.asof_date == asof_date
    assert stored.available_at == daily_price_available_at(asof_date).astimezone(
        timezone.utc
    )
    assert stored.source_metadata["window"]["start_date"] == start_date.isoformat()
    assert stored.source_metadata["window"]["end_date"] == end_date.isoformat()


def test_materialize_momentum_skips_and_reports_insufficient_history() -> None:
    calendar_rows, sessions = _calendar_rows(date(2024, 1, 2), session_count=252)
    asof_date = sessions[-1]
    repository = FakeMomentumRepository(calendar_rows=calendar_rows, prices={})

    summary = materialize_momentum_12_1(
        repository,
        universe_name="falsifier_seed",
        start_date=asof_date,
        end_date=asof_date,
        computed_by_run_id=77,
    )

    assert summary.values_written == 0
    assert summary.skipped_insufficient_history == 1
    assert summary.skipped_missing_price == 0
    assert not repository.feature_values


def test_materialize_momentum_check_command_is_offline() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--check",
            "--universe",
            "falsifier_seed",
            "--start-date",
            "2024-01-02",
            "--end-date",
            "2024-01-03",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "definition hash" in result.stdout
    assert feature_definition_hash(MOMENTUM_12_1_DEFINITION) in result.stdout


def _calendar_rows(
    start: date,
    *,
    session_count: int,
) -> tuple[tuple[TradingCalendarRow, ...], list[date]]:
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
    return tuple(rows), sessions


def _session_close(day: date) -> datetime:
    return datetime.combine(day, time(21), tzinfo=timezone.utc)


def _price(price_date: date, adjusted_close: str) -> AdjustedDailyPriceObservation:
    return AdjustedDailyPriceObservation(
        price_date=price_date,
        adjusted_close=Decimal(adjusted_close),
        available_at=daily_price_available_at(price_date).astimezone(timezone.utc),
    )


class FakeMomentumRepository:
    def __init__(
        self,
        *,
        calendar_rows: tuple[TradingCalendarRow, ...],
        prices: dict[int, list[AdjustedDailyPriceObservation]],
    ) -> None:
        self.definition = FeatureDefinitionRecord(
            id=501,
            name="momentum_12_1",
            version=1,
            kind="numeric",
            computation_spec=dict(MOMENTUM_12_1_DEFINITION.computation_spec),
            definition_hash=feature_definition_hash(MOMENTUM_12_1_DEFINITION),
            notes=None,
        )
        self.policy = AvailableAtPolicyRecord(
            id=3,
            name="daily_price",
            version=1,
            rule={
                "type": "date_at_time",
                "base": "price_date",
                "time": "18:00",
                "timezone": "America/New_York",
            },
        )
        self.memberships = (
            UniverseMembershipRecord(
                security_id=SECURITY_ID,
                ticker="AAPL",
                valid_from=date(2020, 1, 1),
                valid_to=None,
            ),
        )
        self.calendar_rows = calendar_rows
        self.prices = prices
        self.feature_values: dict[tuple[int, date, int], FeatureValueWrite] = {}

    def ensure_feature_definition(
        self,
        definition: object,
        *,
        notes: str | None = None,
    ) -> FeatureDefinitionRecord:
        return self.definition

    def load_available_at_policy(
        self,
        *,
        name: str,
        version: int,
    ) -> AvailableAtPolicyRecord:
        return self.policy

    def load_universe_memberships(
        self,
        *,
        universe_name: str,
        start_date: date | None,
        end_date: date | None,
    ) -> tuple[UniverseMembershipRecord, ...]:
        return self.memberships

    def load_trading_calendar(
        self,
        *,
        end_date: date | None,
    ) -> tuple[TradingCalendarRow, ...]:
        if end_date is None:
            return self.calendar_rows
        return tuple(row for row in self.calendar_rows if row.date <= end_date)

    def load_adjusted_prices(
        self,
        *,
        security_ids: list[int] | tuple[int, ...],
        end_date: date | None,
        available_at_policy_id: int,
    ) -> tuple[tuple[int, AdjustedDailyPriceObservation], ...]:
        rows: list[tuple[int, AdjustedDailyPriceObservation]] = []
        for security_id in security_ids:
            for price in self.prices.get(security_id, []):
                if end_date is None or price.price_date <= end_date:
                    rows.append((security_id, price))
        return tuple(rows)

    def write_feature_values(self, values: Any) -> object:
        for value in values:
            key = (value.security_id, value.asof_date, value.feature_definition_id)
            self.feature_values[key] = value
        return object()
