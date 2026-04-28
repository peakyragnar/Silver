from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import pytest

from silver.backtest.momentum_falsifier import MomentumBacktestRow
from silver.backtest.regimes import (
    RegimeDefinition,
    RegimeDefinitionError,
    RegimeSliceError,
    default_phase_1_regimes,
    regime_for_date,
    rows_for_regime,
    slice_rows_by_regime,
    summarize_by_regime,
    validate_regime_definitions,
)


def test_default_phase_1_regimes_cover_seed_calendar_boundaries() -> None:
    regimes = default_phase_1_regimes()

    assert [regime.name for regime in regimes] == [
        "low_rate_expansion_2014_2015",
        "late_cycle_expansion_2016_2019",
        "covid_shock_recovery_2020_2021",
        "inflation_hike_cycle_2022_2023",
        "recent_seed_window_2024_2026",
    ]
    assert regimes[0].start_date == date(2014, 1, 1)
    assert regimes[-1].end_date == date(2026, 12, 31)
    assert regime_for_date(date(2014, 1, 1)) == regimes[0]
    assert regime_for_date(date(2015, 12, 31)) == regimes[0]
    assert regime_for_date(date(2016, 1, 1)) == regimes[1]
    assert regime_for_date(
        datetime(2026, 12, 31, 23, 59, tzinfo=timezone.utc)
    ) == regimes[-1]
    assert regime_for_date(date(2013, 12, 31)) is None

    for previous, current in zip(regimes, regimes[1:]):
        assert current.start_date == previous.end_date + timedelta(days=1)


def test_regime_validation_rejects_overlaps_and_duplicate_names() -> None:
    with pytest.raises(RegimeDefinitionError, match="must not overlap"):
        validate_regime_definitions(
            (
                _regime("first", date(2024, 1, 1), date(2024, 1, 10)),
                _regime("second", date(2024, 1, 10), date(2024, 1, 20)),
            )
        )

    with pytest.raises(RegimeDefinitionError, match="duplicate regime name"):
        validate_regime_definitions(
            (
                _regime("same", date(2024, 1, 1), date(2024, 1, 10)),
                _regime("same", date(2024, 1, 11), date(2024, 1, 20)),
            )
        )


def test_slice_rows_by_regime_orders_regimes_and_reports_empty_slices() -> None:
    early = _regime("early", date(2024, 1, 1), date(2024, 1, 31))
    middle = _regime("middle", date(2024, 2, 1), date(2024, 2, 29))
    late = _regime("late", date(2024, 3, 1), date(2024, 3, 31))
    rows = (
        _Sample(asof_date=date(2024, 3, 3), value=3.0),
        _Sample(asof_date=date(2024, 1, 2), value=1.0),
    )

    slices = slice_rows_by_regime(
        rows,
        date_getter=lambda row: row.asof_date,
        regimes=(late, middle, early),
    )

    assert [regime_slice.regime_name for regime_slice in slices] == [
        "early",
        "middle",
        "late",
    ]
    assert [regime_slice.sample_count for regime_slice in slices] == [1, 0, 1]
    assert slices[0].rows == (rows[1],)
    assert slices[1].rows == ()
    assert slices[2].rows == (rows[0],)


def test_rows_for_regime_returns_named_rows_and_rejects_unknown_names() -> None:
    early = _regime("early", date(2024, 1, 1), date(2024, 1, 31))
    late = _regime("late", date(2024, 2, 1), date(2024, 2, 29))
    rows = (_Sample(asof_date=date(2024, 1, 2), value=1.0),)

    assert rows_for_regime(
        rows,
        "late",
        date_getter=lambda row: row.asof_date,
        regimes=(early, late),
    ) == ()

    with pytest.raises(RegimeSliceError, match="unknown regime_name"):
        rows_for_regime(
            rows,
            "missing",
            date_getter=lambda row: row.asof_date,
            regimes=(early, late),
        )


def test_rows_outside_configured_regimes_fail_closed() -> None:
    rows = (_Sample(asof_date=date(2030, 1, 1), value=1.0),)

    with pytest.raises(RegimeSliceError, match="outside all regime definitions"):
        slice_rows_by_regime(rows, date_getter=lambda row: row.asof_date)


def test_summarize_by_regime_counts_samples_and_stats_for_backtest_rows() -> None:
    january = _regime("january", date(2024, 1, 1), date(2024, 1, 31))
    february = _regime("february", date(2024, 2, 1), date(2024, 2, 29))
    march = _regime("march", date(2024, 3, 1), date(2024, 3, 31))
    rows = (
        MomentumBacktestRow(
            ticker="AAA",
            asof_date=date(2024, 1, 2),
            horizon_date=date(2024, 1, 5),
            feature_value=1.0,
            realized_return=0.03,
        ),
        MomentumBacktestRow(
            ticker="BBB",
            asof_date=date(2024, 1, 3),
            horizon_date=date(2024, 1, 6),
            feature_value=2.0,
            realized_return=-0.01,
        ),
        MomentumBacktestRow(
            ticker="CCC",
            asof_date=date(2024, 3, 1),
            horizon_date=date(2024, 3, 4),
            feature_value=3.0,
            realized_return=0.02,
        ),
    )

    summaries = summarize_by_regime(
        rows,
        date_getter=lambda row: row.asof_date,
        value_getter=lambda row: row.realized_return,
        regimes=(march, february, january),
    )

    assert [summary.regime_name for summary in summaries] == [
        "january",
        "february",
        "march",
    ]
    assert summaries[0].sample_count == 2
    assert summaries[0].value_count == 2
    assert summaries[0].mean == pytest.approx(0.01)
    assert summaries[0].sample_stddev == pytest.approx(0.0282842712)
    assert summaries[0].minimum == pytest.approx(-0.01)
    assert summaries[0].maximum == pytest.approx(0.03)
    assert summaries[0].hit_rate == pytest.approx(0.5)
    assert summaries[1].sample_count == 0
    assert summaries[1].value_count == 0
    assert summaries[1].mean is None
    assert summaries[1].sample_stddev is None
    assert summaries[1].hit_rate is None
    assert summaries[2].sample_count == 1
    assert summaries[2].sample_stddev is None


@dataclass(frozen=True, slots=True)
class _Sample:
    asof_date: date
    value: float


def _regime(name: str, start_date: date, end_date: date) -> RegimeDefinition:
    return RegimeDefinition(
        name=name,
        start_date=start_date,
        end_date=end_date,
        description=f"manual test regime {name}",
    )
