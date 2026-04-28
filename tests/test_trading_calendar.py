from __future__ import annotations

import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

from silver.time.trading_calendar import (
    CANONICAL_HORIZONS,
    DEFAULT_CONFIG_PATH,
    DEFAULT_SEED_PATH,
    InvalidTradingHorizonError,
    MissingTradingCalendarRowsError,
    TradingCalendar,
    TradingCalendarRow,
    build_upsert_sql,
    generate_trading_calendar,
    load_calendar_config,
    load_seed_csv,
    rows_to_csv,
    validate_complete_calendar,
)


ROOT = Path(__file__).resolve().parents[1]
SEED_SCRIPT = ROOT / "scripts" / "seed_trading_calendar.py"


@pytest.fixture(scope="module")
def phase_1_rows() -> list[TradingCalendarRow]:
    config = load_calendar_config(DEFAULT_CONFIG_PATH)
    return generate_trading_calendar(config)


def test_default_config_generates_complete_phase_1_range(
    phase_1_rows: list[TradingCalendarRow],
) -> None:
    config = load_calendar_config(DEFAULT_CONFIG_PATH)

    validate_complete_calendar(phase_1_rows, config.start_date, config.end_date)

    assert config.canonical_horizons == CANONICAL_HORIZONS
    assert phase_1_rows[0].date == date(2014, 1, 1)
    assert phase_1_rows[-1].date == date(2026, 12, 31)
    assert len(phase_1_rows) == 4_748


def test_generation_marks_weekends_holidays_and_early_closes(
    phase_1_rows: list[TradingCalendarRow],
) -> None:
    rows_by_date = {row.date: row for row in phase_1_rows}

    assert rows_by_date[date(2024, 7, 6)].is_session is False
    assert rows_by_date[date(2024, 7, 4)].is_session is False
    assert rows_by_date[date(2024, 7, 5)].is_session is True
    assert rows_by_date[date(2024, 7, 3)].is_session is True
    assert rows_by_date[date(2024, 7, 3)].is_early_close is True
    assert (
        rows_by_date[date(2024, 7, 3)].session_close.isoformat()
        == "2024-07-03T17:00:00+00:00"
    )


def test_advance_trading_days_skips_weekends_and_market_holidays(
    phase_1_rows: list[TradingCalendarRow],
) -> None:
    calendar = TradingCalendar(phase_1_rows)

    assert calendar.advance_trading_days(date(2024, 1, 12), 5) == date(2024, 1, 22)
    assert calendar.advance_trading_days(date(2024, 7, 2), 5) == date(2024, 7, 10)


def test_advance_canonical_horizons_from_known_asof(
    phase_1_rows: list[TradingCalendarRow],
) -> None:
    calendar = TradingCalendar(phase_1_rows)

    assert calendar.advance_canonical_horizons(date(2024, 1, 2)) == {
        5: date(2024, 1, 9),
        21: date(2024, 2, 1),
        63: date(2024, 4, 3),
        126: date(2024, 7, 3),
        252: date(2025, 1, 2),
    }


def test_advance_rejects_non_canonical_horizon(
    phase_1_rows: list[TradingCalendarRow],
) -> None:
    calendar = TradingCalendar(phase_1_rows)

    with pytest.raises(InvalidTradingHorizonError, match="5, 21, 63, 126, 252"):
        calendar.advance_trading_days(date(2024, 1, 2), 1)


def test_missing_calendar_row_fails_instead_of_approximating() -> None:
    rows = [
        TradingCalendarRow(date(2024, 1, 12), True, _close("2024-01-12T21:00:00+00:00")),
        TradingCalendarRow(date(2024, 1, 13), False, None),
        TradingCalendarRow(date(2024, 1, 14), False, None),
        TradingCalendarRow(date(2024, 1, 16), True, _close("2024-01-16T21:00:00+00:00")),
    ]
    calendar = TradingCalendar(rows)

    with pytest.raises(MissingTradingCalendarRowsError, match="2024-01-15"):
        calendar.advance_trading_days(date(2024, 1, 12), 5)


def test_seed_csv_matches_generated_calendar(
    phase_1_rows: list[TradingCalendarRow],
) -> None:
    seed_rows = load_seed_csv(DEFAULT_SEED_PATH)

    assert rows_to_csv(seed_rows) == rows_to_csv(phase_1_rows)


def test_build_upsert_sql_is_idempotent(
    phase_1_rows: list[TradingCalendarRow],
) -> None:
    sql = build_upsert_sql(phase_1_rows[:3])

    assert "ON CONFLICT (date) DO UPDATE SET" in sql
    assert "silver.trading_calendar.session_close IS DISTINCT FROM" in sql
    assert "'2014-01-01'::date, false, NULL, false" in sql


def test_check_command_validates_generated_seed_file() -> None:
    result = subprocess.run(
        [sys.executable, str(SEED_SCRIPT), "--check"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "trading-calendar row(s) checked" in result.stdout


def _close(value: str) -> datetime:
    return datetime.fromisoformat(value)
