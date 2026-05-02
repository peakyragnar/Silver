from __future__ import annotations

from datetime import datetime, timezone

import pytest

from silver.releases import (
    EarningsReleasePolicy,
    release_available_at,
    release_market_timing,
)
from silver.releases.repository import EarningsReleaseRepositoryError
from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


def test_sec_8k_material_policy_adds_30_minutes() -> None:
    policy = EarningsReleasePolicy(
        id=7,
        name="sec_8k_material",
        version=1,
        rule={
            "type": "timestamp_plus_duration",
            "base": "accepted_at",
            "duration": "PT30M",
        },
    )

    assert release_available_at(
        datetime(2026, 4, 30, 16, 30, 41, tzinfo=timezone.utc),
        policy=policy,
    ) == datetime(2026, 4, 30, 17, 0, 41, tzinfo=timezone.utc)


def test_sec_8k_material_policy_requires_timestamp_base() -> None:
    policy = EarningsReleasePolicy(
        id=7,
        name="sec_8k_material",
        version=1,
        rule={
            "type": "next_trading_session_time_after_timestamp",
            "base": "accepted_at",
            "duration": "PT30M",
        },
    )

    with pytest.raises(EarningsReleaseRepositoryError, match="timestamp_plus_duration"):
        release_available_at(
            datetime(2026, 4, 30, 16, 30, 41, tzinfo=timezone.utc),
            policy=policy,
        )


def test_release_market_timing_classifies_bmo_rth_and_amc() -> None:
    calendar = TradingCalendar(
        (
            TradingCalendarRow(
                date=datetime(2026, 4, 30).date(),
                is_session=True,
                session_close=datetime(2026, 4, 30, 20, 0, tzinfo=timezone.utc),
            ),
        )
    )

    assert (
        release_market_timing(
            datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc),
            calendar=calendar,
        )
        == "bmo"
    )
    assert (
        release_market_timing(
            datetime(2026, 4, 30, 15, 0, tzinfo=timezone.utc),
            calendar=calendar,
        )
        == "rth"
    )
    assert (
        release_market_timing(
            datetime(2026, 4, 30, 20, 30, 41, tzinfo=timezone.utc),
            calendar=calendar,
        )
        == "amc"
    )
