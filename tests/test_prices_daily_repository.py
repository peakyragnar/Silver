from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from silver.prices import (
    DailyPricePersistenceError,
    DailyPriceRepository,
    DailyPriceRow,
    daily_price_available_at,
)


DAILY_PRICE_RULE = {
    "type": "date_at_time",
    "base": "price_date",
    "time": "18:00",
    "timezone": "America/New_York",
    "calendar": "NYSE",
}
WRITE_KWARGS = {
    "raw_object_id": 11,
    "source": "fmp",
    "available_at_policy_id": 3,
    "normalized_by_run_id": 29,
}


def test_daily_price_available_at_uses_policy_timezone() -> None:
    assert daily_price_available_at(date(2024, 1, 2), DAILY_PRICE_RULE) == datetime(
        2024,
        1,
        2,
        23,
        0,
        tzinfo=timezone.utc,
    )
    assert daily_price_available_at(date(2024, 7, 2), DAILY_PRICE_RULE) == datetime(
        2024,
        7,
        2,
        22,
        0,
        tzinfo=timezone.utc,
    )


def test_write_daily_prices_persists_lineage_and_available_at() -> None:
    connection = FakeConnection()
    repository = DailyPriceRepository(connection)

    result = repository.write_daily_prices(
        [
            _price_row(date(2024, 1, 3), close="184.25"),
            _price_row(date(2024, 1, 2), close="185.64"),
        ],
        **WRITE_KWARGS,
    )

    assert result.rows_written == 2
    assert result.tickers == ("AAPL",)
    assert result.dates == (date(2024, 1, 2), date(2024, 1, 3))
    assert result.raw_object_id == 11
    assert result.available_at_policy_id == 3
    assert result.normalized_by_run_id == 29
    assert result.normalization_version == "fmp_daily_prices_v1"

    rows = sorted(connection.prices_daily.values(), key=lambda row: row["date"])
    assert [row["date"] for row in rows] == [date(2024, 1, 2), date(2024, 1, 3)]
    assert rows[0]["security_id"] == 101
    assert rows[0]["source_system"] == "fmp"
    assert rows[0]["normalization_version"] == "fmp_daily_prices_v1"
    assert rows[0]["normalized_by_run_id"] == 29
    assert rows[0]["raw_object_id"] == 11
    assert rows[0]["available_at_policy_id"] == 3
    assert rows[0]["available_at"] == datetime(
        2024,
        1,
        2,
        23,
        0,
        tzinfo=timezone.utc,
    )


def test_write_daily_prices_is_idempotent_for_same_lineage() -> None:
    connection = FakeConnection()
    repository = DailyPriceRepository(connection)
    row = _price_row(date(2024, 1, 2), close="185.64")

    first = repository.write_daily_prices(
        [row],
        **WRITE_KWARGS,
    )
    second = repository.write_daily_prices(
        [row],
        **WRITE_KWARGS,
    )

    assert first == second
    assert len(connection.prices_daily) == 1
    assert connection.price_write_count == 2
    [stored] = connection.prices_daily.values()
    assert stored["close"] == Decimal("185.64")


def test_write_daily_prices_upsert_relinks_identical_rows_to_new_run() -> None:
    connection = FakeConnection()
    repository = DailyPriceRepository(connection)
    row = _price_row(date(2024, 1, 2), close="185.64")

    repository.write_daily_prices([row], **WRITE_KWARGS)
    retry_kwargs = dict(WRITE_KWARGS)
    retry_kwargs["normalized_by_run_id"] = 30
    repository.write_daily_prices([row], **retry_kwargs)

    [stored] = connection.prices_daily.values()
    assert stored["normalized_by_run_id"] == 30
    insert_sql = next(
        sql
        for sql, _params in reversed(connection.executed)
        if sql.startswith("INSERT INTO silver.prices_daily")
    )
    assert "silver.prices_daily.normalized_by_run_id IS DISTINCT FROM" in insert_sql


@pytest.mark.parametrize(
    ("kwargs", "error"),
    (
        ({"raw_object_id": 0}, "raw_object_id must be a positive integer"),
        ({"source": ""}, "source must be a non-empty string"),
        (
            {"available_at_policy_id": 0},
            "available_at_policy_id must be a positive integer",
        ),
        (
            {"normalized_by_run_id": 0},
            "normalized_by_run_id must be a positive integer",
        ),
    ),
)
def test_write_daily_prices_rejects_missing_required_lineage(
    kwargs: dict[str, object],
    error: str,
) -> None:
    connection = FakeConnection()
    call_kwargs = dict(WRITE_KWARGS)
    call_kwargs.update(kwargs)

    with pytest.raises(DailyPricePersistenceError, match=error):
        DailyPriceRepository(connection).write_daily_prices(
            [_price_row(date(2024, 1, 2))],
            **call_kwargs,  # type: ignore[arg-type]
        )

    assert not connection.prices_daily


def test_write_daily_prices_rejects_missing_policy_before_write() -> None:
    connection = FakeConnection(policies={})

    with pytest.raises(
        DailyPricePersistenceError,
        match="available_at policy 3 was not found",
    ):
        DailyPriceRepository(connection).write_daily_prices(
            [_price_row(date(2024, 1, 2))],
            **WRITE_KWARGS,
        )

    assert not connection.prices_daily


def test_write_daily_prices_rejects_missing_security_before_write() -> None:
    connection = FakeConnection(securities={})

    with pytest.raises(
        DailyPricePersistenceError,
        match="security not found for ticker AAPL",
    ):
        DailyPriceRepository(connection).write_daily_prices(
            [_price_row(date(2024, 1, 2))],
            **WRITE_KWARGS,
        )

    assert not connection.prices_daily


def test_write_daily_prices_rejects_non_session_date_before_write() -> None:
    connection = FakeConnection(calendar={date(2024, 1, 6): False})

    with pytest.raises(
        DailyPricePersistenceError,
        match="price date must be a trading session; got 2024-01-06",
    ):
        DailyPriceRepository(connection).write_daily_prices(
            [_price_row(date(2024, 1, 6))],
            **WRITE_KWARGS,
        )

    assert not connection.prices_daily


def test_write_daily_prices_rejects_missing_calendar_date_before_write() -> None:
    connection = FakeConnection(calendar={})

    with pytest.raises(
        DailyPricePersistenceError,
        match="trading-calendar row is missing for price date 2024-01-02",
    ):
        DailyPriceRepository(connection).write_daily_prices(
            [_price_row(date(2024, 1, 2))],
            **WRITE_KWARGS,
        )

    assert not connection.prices_daily


def test_write_daily_prices_rejects_source_mismatch_before_write() -> None:
    connection = FakeConnection()

    with pytest.raises(
        DailyPricePersistenceError,
        match="daily price row source fmp does not match write source sec",
    ):
        DailyPriceRepository(connection).write_daily_prices(
            [_price_row(date(2024, 1, 2))],
            raw_object_id=11,
            source="sec",
            available_at_policy_id=3,
            normalized_by_run_id=29,
        )

    assert not connection.prices_daily


def test_load_daily_price_policy_returns_versioned_policy() -> None:
    policy = DailyPriceRepository(FakeConnection()).load_daily_price_policy()

    assert policy.id == 3
    assert policy.name == "daily_price"
    assert policy.version == 1
    assert policy.rule == DAILY_PRICE_RULE


def _price_row(day: date, *, close: str = "185.64") -> DailyPriceRow:
    return DailyPriceRow(
        ticker="AAPL",
        date=day,
        open=Decimal("187.15"),
        high=Decimal("188.44"),
        low=Decimal("183.89"),
        close=Decimal(close),
        adj_close=Decimal("184.68"),
        volume=82488700,
        source="fmp",
        raw_metadata={"source_date": day.isoformat(), "source_symbol": "AAPL"},
    )


class FakeConnection:
    def __init__(
        self,
        *,
        securities: dict[str, int] | None = None,
        calendar: dict[date, bool] | None = None,
        policies: dict[int, dict[str, Any]] | None = None,
    ) -> None:
        self.securities = securities if securities is not None else {"AAPL": 101}
        self.calendar = (
            calendar
            if calendar is not None
            else {date(2024, 1, 2): True, date(2024, 1, 3): True}
        )
        self.policies = (
            policies
            if policies is not None
            else {
                3: {
                    "id": 3,
                    "name": "daily_price",
                    "version": 1,
                    "rule": DAILY_PRICE_RULE,
                }
            }
        )
        self.prices_daily: dict[tuple[int, date], dict[str, Any]] = {}
        self.executed: list[tuple[str, dict[str, Any]]] = []
        self.price_write_count = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)


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
        if sql.startswith("SELECT id, name, version, rule"):
            self._select_policy(params)
            return
        if sql.startswith("SELECT id\nFROM silver.securities"):
            self._select_security(params)
            return
        if sql.startswith("SELECT date, is_session"):
            self._select_calendar(params)
            return
        if sql.startswith("INSERT INTO silver.prices_daily"):
            self._upsert_price(params)
            return
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self) -> dict[str, Any] | None:
        return self._one

    def fetchall(self) -> list[dict[str, Any]]:
        return self._many

    def _select_policy(self, params: dict[str, Any]) -> None:
        if "available_at_policy_id" in params:
            policy_id = params["available_at_policy_id"]
            self._one = self.connection.policies.get(policy_id)
            return
        self._one = next(
            (
                policy
                for policy in self.connection.policies.values()
                if policy["name"] == params["name"]
                and policy["version"] == params["version"]
            ),
            None,
        )

    def _select_security(self, params: dict[str, Any]) -> None:
        security_id = self.connection.securities.get(params["ticker"])
        self._one = None if security_id is None else {"id": security_id}

    def _select_calendar(self, params: dict[str, Any]) -> None:
        self._many = [
            {"date": price_date, "is_session": self.connection.calendar[price_date]}
            for price_date in params["dates"]
            if price_date in self.connection.calendar
        ]

    def _upsert_price(self, params: dict[str, Any]) -> None:
        self.connection.price_write_count += 1
        key = (
            params["security_id"],
            params["date"],
        )
        self.connection.prices_daily[key] = dict(params)
