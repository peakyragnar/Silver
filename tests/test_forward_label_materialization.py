from __future__ import annotations

import json
import importlib.util
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from silver.labels.forward_returns import SkipReason
from silver.labels.materialize import build_forward_label_records
from silver.labels.repository import (
    ForwardLabelPriceObservation,
    ForwardLabelRecord,
    ForwardLabelRepository,
)
from silver.prices.daily import DailyPriceRow
from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


ROOT = Path(__file__).resolve().parents[1]
CLI_PATH = ROOT / "scripts" / "materialize_forward_labels.py"
CLI_SPEC = importlib.util.spec_from_file_location(
    "materialize_forward_labels_cli",
    CLI_PATH,
)
assert CLI_SPEC is not None
assert CLI_SPEC.loader is not None
cli = importlib.util.module_from_spec(CLI_SPEC)
sys.modules[CLI_SPEC.name] = cli
CLI_SPEC.loader.exec_module(cli)


def test_build_records_uses_target_price_availability_and_surfaces_skips() -> None:
    calendar = _calendar_with_sessions(8)
    sessions = _sessions(calendar)
    prices = [
        _price_observation(sessions[0], adj_close="100", available_hour=23),
        _price_observation(sessions[1], adj_close="101", available_hour=23),
        _price_observation(sessions[5], adj_close="105", available_hour=23),
    ]

    result = build_forward_label_records(
        prices=prices,
        calendar=calendar,
        label_dates_by_security={101: (sessions[0], sessions[1])},
        computed_by_run_id=7,
        horizons=(5,),
    )

    assert len(result.records) == 1
    record = result.records[0]
    assert record.security_id == 101
    assert record.label_date == sessions[0]
    assert record.horizon_date == sessions[5]
    assert record.horizon_close_at == calendar.row_for(sessions[5]).session_close
    assert record.available_at == prices[2].available_at
    assert record.available_at_policy_id == prices[2].available_at_policy_id
    assert record.realized_raw_return == Decimal("0.05")
    assert record.benchmark_security_id is None
    assert record.realized_excess_return is None
    assert "benchmark" not in record.metadata
    assert record.metadata["target_price_available_at"] == prices[2].available_at.isoformat()

    assert len(result.skipped) == 1
    skipped = result.skipped[0]
    assert skipped.reason is SkipReason.MISSING_TARGET_PRICE
    assert skipped.asof_date == sessions[1]
    assert skipped.target_date == sessions[6]


def test_build_records_populates_benchmark_relative_fields_and_pit_availability() -> None:
    calendar = _calendar_with_sessions(8)
    sessions = _sessions(calendar)
    prices = [
        _price_observation(sessions[0], adj_close="100", available_hour=23),
        _price_observation(sessions[5], adj_close="110", available_hour=23),
    ]
    benchmark_prices = [
        _price_observation(
            sessions[0],
            adj_close="200",
            available_hour=23,
            security_id=202,
            ticker="SPY",
            policy_id=4,
        ),
        _price_observation(
            sessions[5],
            adj_close="204",
            available_hour=30,
            security_id=202,
            ticker="SPY",
            policy_id=4,
        ),
    ]

    result = build_forward_label_records(
        prices=prices,
        benchmark_prices=benchmark_prices,
        calendar=calendar,
        label_dates_by_security={101: (sessions[0],)},
        computed_by_run_id=7,
        horizons=(5,),
    )

    assert result.skipped == ()
    assert len(result.records) == 1
    record = result.records[0]
    assert record.benchmark_security_id == 202
    assert record.realized_raw_return == Decimal("0.1")
    assert record.realized_excess_return == Decimal("0.08")
    assert record.available_at == benchmark_prices[1].available_at
    assert record.available_at_policy_id == 4
    assert record.metadata["benchmark"] == {
        "ticker": "SPY",
        "security_id": 202,
        "asof_date": sessions[0].isoformat(),
        "target_date": sessions[5].isoformat(),
        "status": "covered",
        "start_price_available_at": benchmark_prices[0].available_at.isoformat(),
        "start_price_available_at_policy_id": 4,
        "target_price_available_at": benchmark_prices[1].available_at.isoformat(),
        "target_price_available_at_policy_id": 4,
        "benchmark_forward_return": "0.02",
    }


def test_filters_price_observations_that_are_not_available_yet() -> None:
    cutoff = datetime(2026, 4, 30, 16, tzinfo=timezone.utc)
    available = _price_observation(
        date(2026, 4, 29),
        adj_close="100",
        available_hour=12,
    )
    unavailable = _price_observation(
        date(2026, 4, 30),
        adj_close="105",
        available_hour=23,
    )

    filtered = cli._available_price_observations((available, unavailable), cutoff)

    assert filtered == (available,)


def test_build_records_marks_missing_benchmark_coverage_without_zero_fill() -> None:
    calendar = _calendar_with_sessions(8)
    sessions = _sessions(calendar)
    prices = [
        _price_observation(sessions[0], adj_close="100", available_hour=23),
        _price_observation(sessions[5], adj_close="110", available_hour=23),
    ]
    benchmark_prices = [
        _price_observation(
            sessions[0],
            adj_close="200",
            available_hour=23,
            security_id=202,
            ticker="SPY",
            policy_id=4,
        ),
    ]

    result = build_forward_label_records(
        prices=prices,
        benchmark_prices=benchmark_prices,
        calendar=calendar,
        label_dates_by_security={101: (sessions[0],)},
        computed_by_run_id=7,
        horizons=(5,),
    )

    assert result.skipped == ()
    assert len(result.records) == 1
    record = result.records[0]
    assert record.benchmark_security_id == 202
    assert record.realized_raw_return == Decimal("0.1")
    assert record.realized_excess_return is None
    assert record.metadata["benchmark"] == {
        "ticker": "SPY",
        "security_id": 202,
        "asof_date": sessions[0].isoformat(),
        "target_date": sessions[5].isoformat(),
        "status": "missing_target_price",
        "missing_price_date": sessions[5].isoformat(),
    }


def test_write_forward_labels_is_idempotent_and_uses_expected_upsert_shape() -> None:
    connection = FakeConnection()
    repository = ForwardLabelRepository(connection)
    record = _label_record()

    first = repository.write_forward_labels([record])
    second = repository.write_forward_labels([record])

    assert first.records_seen == 1
    assert first.rows_changed == 1
    assert second.records_seen == 1
    assert second.rows_changed == 0
    assert len(connection.forward_labels) == 1

    insert_sql = next(
        sql
        for sql, _params in connection.executed
        if sql.startswith("INSERT INTO silver.forward_return_labels")
    )
    assert "ON CONFLICT (security_id, label_date, horizon_days, label_version)" in insert_sql
    assert "silver.forward_return_labels.available_at IS DISTINCT FROM" in insert_sql
    assert "RETURNING id" in insert_sql


def test_repository_loads_universe_prices_and_pit_label_dates() -> None:
    connection = FakeConnection()
    repository = ForwardLabelRepository(connection)

    prices = repository.load_universe_price_observations(
        universe_name="falsifier_seed",
        label_start_date=date(2024, 1, 2),
        label_end_date=date(2024, 1, 31),
        price_end_date=date(2024, 2, 29),
    )
    label_dates = repository.load_label_dates_by_security(
        universe_name="falsifier_seed",
        label_start_date=date(2024, 1, 2),
        label_end_date=date(2024, 1, 31),
    )

    assert len(prices) == 1
    assert prices[0].security_id == 101
    assert prices[0].row.ticker == "AAPL"
    assert prices[0].row.adj_close == Decimal("184.68")
    assert label_dates == {101: (date(2024, 1, 2),)}

    price_sql = next(
        sql
        for sql, _params in connection.executed
        if sql.startswith("WITH member_securities")
    )
    label_dates_sql = next(
        sql
        for sql, _params in connection.executed
        if sql.startswith("SELECT DISTINCT p.security_id")
    )
    assert "FROM silver.prices_daily AS p" in price_sql
    assert "FROM silver.universe_membership" in price_sql
    assert "JOIN silver.analytics_runs AS run" in price_sql
    assert "run.id = p.normalized_by_run_id" in price_sql
    assert "run.status = 'succeeded'" in price_sql
    assert "%(label_end_date)s::date IS NULL" in price_sql
    assert "valid_to >= %(label_start_date)s::date" in price_sql
    assert "%(price_start_date)s::date IS NULL" in price_sql
    assert "p.date <= %(price_end_date)s::date" in price_sql
    assert "um.valid_from <= p.date" in label_dates_sql
    assert "um.valid_to IS NULL OR um.valid_to >= p.date" in label_dates_sql
    assert "JOIN silver.analytics_runs AS run" in label_dates_sql
    assert "run.id = p.normalized_by_run_id" in label_dates_sql
    assert "run.status = 'succeeded'" in label_dates_sql
    assert "%(label_start_date)s::date IS NULL" in label_dates_sql
    assert "p.date <= %(label_end_date)s::date" in label_dates_sql


def test_repository_loads_benchmark_prices_without_universe_membership() -> None:
    connection = FakeConnection()
    repository = ForwardLabelRepository(connection)

    prices = repository.load_security_price_observations(
        ticker="spy",
        price_start_date=date(2024, 1, 2),
        price_end_date=date(2024, 1, 31),
    )

    assert len(prices) == 1
    assert prices[0].security_id == 202
    assert prices[0].row.ticker == "SPY"
    benchmark_sql = next(
        sql for sql, params in connection.executed if params.get("ticker") == "SPY"
    )
    assert "FROM silver.prices_daily AS p" in benchmark_sql
    assert "JOIN silver.analytics_runs AS run" in benchmark_sql
    assert "run.id = p.normalized_by_run_id" in benchmark_sql
    assert "run.status = 'succeeded'" in benchmark_sql
    assert "AND upper(s.ticker) = upper(%(ticker)s)" in benchmark_sql
    assert "%(price_start_date)s::date IS NULL" in benchmark_sql
    assert "p.date <= %(price_end_date)s::date" in benchmark_sql
    assert "universe_membership" not in benchmark_sql


def test_check_command_runs_without_database_url() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "materialize_forward_labels.py"), "--check"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "OK: materialize_forward_labels check passed" in result.stdout


def test_check_command_accepts_benchmark_ticker_without_database_url() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "materialize_forward_labels.py"),
            "--check",
            "--benchmark-ticker",
            "spy",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "OK: materialize_forward_labels check passed" in result.stdout


def _label_record() -> ForwardLabelRecord:
    label_date = date(2024, 1, 2)
    horizon_date = date(2024, 1, 9)
    horizon_close_at = datetime(2024, 1, 9, 21, tzinfo=timezone.utc)
    available_at = datetime(2024, 1, 9, 23, tzinfo=timezone.utc)
    return ForwardLabelRecord(
        security_id=101,
        label_date=label_date,
        horizon_days=5,
        horizon_date=horizon_date,
        horizon_close_at=horizon_close_at,
        label_version=1,
        start_adj_close=Decimal("100"),
        end_adj_close=Decimal("105"),
        realized_raw_return=Decimal("0.05"),
        benchmark_security_id=None,
        realized_excess_return=None,
        available_at=available_at,
        available_at_policy_id=3,
        computed_by_run_id=7,
        metadata={"source": "fixture"},
    )


def _calendar_with_sessions(session_count: int) -> TradingCalendar:
    rows: list[TradingCalendarRow] = []
    current = date(2024, 1, 2)
    sessions_seen = 0
    while sessions_seen < session_count:
        is_session = current.weekday() < 5
        rows.append(
            TradingCalendarRow(
                date=current,
                is_session=is_session,
                session_close=(
                    datetime.combine(current, datetime.min.time(), tzinfo=timezone.utc)
                    + timedelta(hours=21)
                    if is_session
                    else None
                ),
            )
        )
        if is_session:
            sessions_seen += 1
        current += timedelta(days=1)
    return TradingCalendar(rows)


def _sessions(calendar: TradingCalendar) -> list[date]:
    return [row.date for row in calendar.rows if row.is_session]


def _price_observation(
    day: date,
    *,
    adj_close: str,
    available_hour: int,
    security_id: int = 101,
    ticker: str = "AAA",
    policy_id: int = 3,
) -> ForwardLabelPriceObservation:
    value = Decimal(adj_close)
    return ForwardLabelPriceObservation(
        security_id=security_id,
        row=DailyPriceRow(
            ticker=ticker,
            date=day,
            open=value,
            high=value,
            low=value,
            close=value,
            adj_close=value,
            volume=1000,
            source="fixture",
        ),
        available_at=datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
        + timedelta(hours=available_hour),
        available_at_policy_id=policy_id,
    )


class FakeConnection:
    def __init__(self) -> None:
        self.executed: list[tuple[str, dict[str, Any]]] = []
        self.forward_labels: dict[tuple[int, date, int, int], dict[str, Any]] = {}
        self._next_label_id = 1

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def next_label_id(self) -> int:
        label_id = self._next_label_id
        self._next_label_id += 1
        return label_id


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self._one: dict[str, Any] | None = None
        self._many: list[dict[str, Any]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: dict[str, Any]) -> None:
        self.connection.executed.append((sql, dict(params)))
        self._one = None
        self._many = []
        if sql.startswith("INSERT INTO silver.forward_return_labels"):
            self._upsert_forward_label(params)
            return
        if sql.startswith("WITH member_securities"):
            self._many = [_price_row()]
            return
        if sql.startswith("SELECT\n    p.security_id"):
            self._many = [_benchmark_price_row()]
            return
        if sql.startswith("SELECT DISTINCT p.security_id"):
            self._many = [{"security_id": 101, "date": date(2024, 1, 2)}]
            return
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self) -> dict[str, Any] | None:
        return self._one

    def fetchall(self) -> list[dict[str, Any]]:
        return self._many

    def _upsert_forward_label(self, params: dict[str, Any]) -> None:
        key = (
            params["security_id"],
            params["label_date"],
            params["horizon_days"],
            params["label_version"],
        )
        row = dict(params)
        row["metadata"] = json.loads(params["metadata"])
        existing = self.connection.forward_labels.get(key)
        if existing is None:
            row["id"] = self.connection.next_label_id()
            self.connection.forward_labels[key] = row
            self._one = {"id": row["id"]}
            return
        if _label_row_changed(existing, row):
            row["id"] = existing["id"]
            self.connection.forward_labels[key] = row
            self._one = {"id": row["id"]}


def _label_row_changed(existing: dict[str, Any], incoming: dict[str, Any]) -> bool:
    compared_fields = (
        "horizon_date",
        "horizon_close_at",
        "start_adj_close",
        "end_adj_close",
        "realized_raw_return",
        "benchmark_security_id",
        "realized_excess_return",
        "available_at",
        "available_at_policy_id",
        "metadata",
    )
    return any(existing[field] != incoming[field] for field in compared_fields)


def _price_row() -> dict[str, Any]:
    return {
        "security_id": 101,
        "ticker": "AAPL",
        "date": date(2024, 1, 2),
        "open": Decimal("187.15"),
        "high": Decimal("188.44"),
        "low": Decimal("183.89"),
        "close": Decimal("185.64"),
        "adj_close": Decimal("184.68"),
        "volume": 82488700,
        "source_system": "fmp",
        "available_at": datetime(2024, 1, 2, 23, tzinfo=timezone.utc),
        "available_at_policy_id": 3,
    }


def _benchmark_price_row() -> dict[str, Any]:
    return {
        "security_id": 202,
        "ticker": "SPY",
        "date": date(2024, 1, 2),
        "open": Decimal("475.15"),
        "high": Decimal("476.44"),
        "low": Decimal("473.89"),
        "close": Decimal("475.64"),
        "adj_close": Decimal("474.68"),
        "volume": 72112000,
        "source_system": "fmp",
        "available_at": datetime(2024, 1, 2, 23, tzinfo=timezone.utc),
        "available_at_policy_id": 3,
    }
