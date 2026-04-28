from __future__ import annotations

import importlib.util
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from silver.ingest import FmpPriceIngestError, RawVault, ingest_fmp_prices
from silver.ingest.raw_vault import REDACTED_VALUE
from silver.sources.fmp import FMPClient, FMPTransportResponse


CLI_PATH = Path(__file__).resolve().parents[1] / "scripts" / "ingest_fmp_prices.py"
CLI_SPEC = importlib.util.spec_from_file_location("ingest_fmp_prices_cli", CLI_PATH)
assert CLI_SPEC is not None
assert CLI_SPEC.loader is not None
cli = importlib.util.module_from_spec(CLI_SPEC)
CLI_SPEC.loader.exec_module(cli)

SEED_TICKERS = ("AAPL", "GOOGL", "JPM", "MSFT", "NVDA")
DAILY_PRICE_RULE = {
    "type": "date_at_time",
    "base": "price_date",
    "time": "18:00",
    "timezone": "America/New_York",
    "calendar": "NYSE",
}


def test_ingest_captures_raw_before_persisting_seed_prices() -> None:
    connection = FakeConnection()
    client = _client(connection, _responses(SEED_TICKERS))

    result = ingest_fmp_prices(
        connection=connection,
        client=client,
        universe="falsifier_seed",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        code_git_sha="abc1234",
    )

    assert result.tickers == SEED_TICKERS
    assert result.raw_responses_captured == 5
    assert result.rows_written == 5
    assert len(connection.raw_objects) == 5
    assert len(connection.prices_daily) == 5
    assert connection.analytics_runs[0]["status"] == "succeeded"

    for ticker in SEED_TICKERS:
        raw_index = connection.events.index(f"raw:{ticker}")
        price_index = connection.events.index(f"price:{ticker}")
        assert raw_index < price_index
        assert "commit" in connection.events[raw_index:price_index]

    for row in connection.raw_objects:
        assert row["params"]["apikey"] == REDACTED_VALUE
        assert "real-secret" not in row["request_url"]


def test_duplicate_ingest_reuses_raw_objects_and_price_rows() -> None:
    connection = FakeConnection()

    first = ingest_fmp_prices(
        connection=connection,
        client=_client(connection, _responses(SEED_TICKERS)),
        universe="falsifier_seed",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        code_git_sha="abc1234",
    )
    second = ingest_fmp_prices(
        connection=connection,
        client=_client(connection, _responses(SEED_TICKERS)),
        universe="falsifier_seed",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        code_git_sha="abc1234",
    )

    assert first.rows_written == second.rows_written == 5
    assert len(connection.raw_objects) == 5
    assert len(connection.prices_daily) == 5
    assert all(not ticker.raw_inserted for ticker in second.ticker_results)


def test_dry_run_reads_persisted_universe_without_fetching_or_writing() -> None:
    connection = FakeConnection()

    result = ingest_fmp_prices(
        connection=connection,
        client=None,
        universe="falsifier_seed",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        code_git_sha="abc1234",
        dry_run=True,
    )

    assert result.dry_run is True
    assert result.tickers == SEED_TICKERS
    assert connection.raw_objects == []
    assert connection.prices_daily == {}
    assert connection.analytics_runs == []


def test_parse_failure_keeps_raw_capture_and_marks_run_failed() -> None:
    connection = FakeConnection(tickers=("AAPL",))
    client = _client(
        connection,
        [FMPTransportResponse(status_code=200, body=b"not json", headers={})],
    )

    with pytest.raises(FmpPriceIngestError, match="not valid JSON"):
        ingest_fmp_prices(
            connection=connection,
            client=client,
            universe="falsifier_seed",
            start_date=date(2024, 1, 2),
            end_date=date(2024, 1, 2),
            code_git_sha="abc1234",
        )

    assert len(connection.raw_objects) == 1
    assert connection.analytics_runs[0]["status"] == "failed"
    assert "commit" in connection.events[
        connection.events.index("raw:AAPL") : connection.events.index("run:failed")
    ]


def test_check_config_uses_seed_reference_data() -> None:
    message = cli.check_config(
        universe="falsifier_seed",
        start_date=None,
        end_date=None,
        seed_config_path=cli.DEFAULT_CONFIG_PATH,
        today=date(2026, 4, 28),
    )

    assert "5 seed ticker(s): AAPL, GOOGL, JPM, MSFT, NVDA" in message
    assert "2014-04-03..2026-04-28" in message


def test_missing_database_url_fails_clearly(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("FMP_API_KEY", "real-secret")

    code = cli.main(["--start-date", "2024-01-02", "--end-date", "2024-01-02"])

    assert code == 1
    captured = capsys.readouterr()
    assert "DATABASE_URL is required" in captured.err
    assert "real-secret" not in captured.err


def test_missing_fmp_api_key_fails_before_database_connection(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = "postgresql://user:password@localhost:5432/silver"
    monkeypatch.delenv("FMP_API_KEY", raising=False)

    code = cli.main(
        [
            "--database-url",
            database_url,
            "--start-date",
            "2024-01-02",
            "--end-date",
            "2024-01-02",
        ]
    )

    assert code == 1
    captured = capsys.readouterr()
    assert "FMP_API_KEY is required" in captured.err
    assert database_url not in captured.err


def _client(
    connection: FakeConnection,
    responses: list[FMPTransportResponse],
) -> FMPClient:
    return FMPClient(
        api_key="real-secret",
        raw_vault=RawVault(connection),
        transport=FakeTransport(responses),
        now=lambda: datetime(2026, 4, 28, 12, 0, tzinfo=timezone.utc),
        sleep=lambda _seconds: None,
    )


def _responses(tickers: tuple[str, ...]) -> list[FMPTransportResponse]:
    return [
        FMPTransportResponse(
            status_code=200,
            body=_payload(ticker),
            headers={"Content-Type": "application/json"},
        )
        for ticker in tickers
    ]


def _payload(ticker: str) -> bytes:
    return json.dumps(
        {
            "symbol": ticker,
            "historical": [
                {
                    "date": "2024-01-02",
                    "open": 100.0,
                    "high": 110.0,
                    "low": 95.0,
                    "close": 105.0,
                    "adjClose": 104.0,
                    "volume": 123456,
                }
            ],
        }
    ).encode("utf-8")


class FakeTransport:
    def __init__(self, responses: list[FMPTransportResponse]) -> None:
        self._responses = list(responses)

    def get(self, url: str, *, timeout: float) -> FMPTransportResponse:
        if not self._responses:
            raise AssertionError(f"unexpected request: {url} {timeout}")
        return self._responses.pop(0)


class FakeConnection:
    def __init__(self, *, tickers: tuple[str, ...] = SEED_TICKERS) -> None:
        self.securities = {
            ticker: security_id
            for security_id, ticker in enumerate(tickers, start=101)
        }
        self.memberships = [
            {
                "security_id": security_id,
                "ticker": ticker,
                "universe_name": "falsifier_seed",
                "valid_from": date(2014, 4, 3),
                "valid_to": None,
            }
            for ticker, security_id in self.securities.items()
        ]
        self.calendar = {date(2024, 1, 2): True}
        self.policies = {
            3: {
                "id": 3,
                "name": "daily_price",
                "version": 1,
                "rule": DAILY_PRICE_RULE,
            }
        }
        self.raw_objects: list[dict[str, Any]] = []
        self.prices_daily: dict[tuple[int, date], dict[str, Any]] = {}
        self.analytics_runs: list[dict[str, Any]] = []
        self.events: list[str] = []
        self.commits = 0
        self.rollbacks = 0
        self._next_raw_id = 1
        self._next_run_id = 1

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1
        self.events.append("commit")

    def rollback(self) -> None:
        self.rollbacks += 1
        self.events.append("rollback")

    def next_raw_id(self) -> int:
        raw_object_id = self._next_raw_id
        self._next_raw_id += 1
        return raw_object_id

    def next_run_id(self) -> int:
        run_id = self._next_run_id
        self._next_run_id += 1
        return run_id


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self._one: dict[str, Any] | tuple[int] | None = None
        self._many: list[dict[str, Any]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: dict[str, Any]) -> None:
        if sql.startswith("SELECT\n    security.id AS security_id"):
            self._select_members(params)
            return
        if sql.startswith("SELECT id, name, version, rule"):
            self._select_policy(params)
            return
        if sql.startswith("INSERT INTO silver.analytics_runs"):
            self._insert_run(params)
            return
        if sql.startswith("UPDATE silver.analytics_runs"):
            self._finish_run(params)
            return
        if sql.startswith("INSERT INTO silver.raw_objects"):
            self._insert_raw(params)
            return
        if sql.startswith("SELECT id\nFROM silver.raw_objects"):
            self._select_raw(params)
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

    def fetchone(self) -> dict[str, Any] | tuple[int] | None:
        return self._one

    def fetchall(self) -> list[dict[str, Any]]:
        return self._many

    def _select_members(self, params: dict[str, Any]) -> None:
        start_date = params["start_date"]
        end_date = params["end_date"]
        self._many = [
            membership
            for membership in sorted(
                self.connection.memberships,
                key=lambda item: (item["ticker"], item["valid_from"]),
            )
            if membership["universe_name"] == params["universe_name"]
            and (end_date is None or membership["valid_from"] <= end_date)
            and (
                start_date is None
                or membership["valid_to"] is None
                or membership["valid_to"] >= start_date
            )
        ]

    def _select_policy(self, params: dict[str, Any]) -> None:
        if "available_at_policy_id" in params:
            self._one = self.connection.policies.get(params["available_at_policy_id"])
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

    def _insert_run(self, params: dict[str, Any]) -> None:
        row = {
            "id": self.connection.next_run_id(),
            "run_kind": params["run_kind"],
            "status": "running",
            "parameters": json.loads(params["parameters"]),
        }
        self.connection.analytics_runs.append(row)
        self.connection.events.append("run:create")
        self._one = row

    def _finish_run(self, params: dict[str, Any]) -> None:
        run_id = params["run_id"]
        for row in self.connection.analytics_runs:
            if row["id"] == run_id:
                row["status"] = params["status"]
                self.connection.events.append(f"run:{params['status']}")
                self._one = row
                return
        self._one = None

    def _insert_raw(self, params: dict[str, Any]) -> None:
        existing = self._find_raw(params)
        if existing is not None:
            self._one = None
            return
        row = dict(params)
        row["id"] = self.connection.next_raw_id()
        row["params"] = json.loads(params["params"])
        row["metadata"] = json.loads(params["metadata"])
        self.connection.raw_objects.append(row)
        self.connection.events.append(f"raw:{row['params']['symbol']}")
        self._one = (row["id"],)

    def _select_raw(self, params: dict[str, Any]) -> None:
        existing = self._find_raw(params)
        self._one = None if existing is None else (existing["id"],)

    def _find_raw(self, params: dict[str, Any]) -> dict[str, Any] | None:
        for row in self.connection.raw_objects:
            if (
                row["vendor"] == params["vendor"]
                and row["endpoint"] == params["endpoint"]
                and row["params_hash"] == params["params_hash"]
                and row["raw_hash"] == params["raw_hash"]
            ):
                return row
        return None

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
        key = (params["security_id"], params["date"])
        self.connection.prices_daily[key] = dict(params)
        ticker = _ticker_for_security(self.connection, params["security_id"])
        self.connection.events.append(f"price:{ticker}")


def _ticker_for_security(connection: FakeConnection, security_id: int) -> str:
    for ticker, candidate_id in connection.securities.items():
        if candidate_id == security_id:
            return ticker
    raise AssertionError(f"unknown security_id: {security_id}")
