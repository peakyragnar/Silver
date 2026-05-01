from __future__ import annotations

import importlib.util
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from silver.ingest import (
    RawVault,
    SecCompanyFactsIngestError,
    ingest_sec_companyfacts,
)
from silver.sources.sec import SECClient, SECHTTPError, SECTransportResponse


CLI_PATH = Path(__file__).resolve().parents[1] / "scripts" / "ingest_sec_companyfacts.py"
CLI_SPEC = importlib.util.spec_from_file_location("ingest_sec_companyfacts_cli", CLI_PATH)
assert CLI_SPEC is not None
assert CLI_SPEC.loader is not None
cli = importlib.util.module_from_spec(CLI_SPEC)
CLI_SPEC.loader.exec_module(cli)

SEED_TICKERS = ("AAPL", "MSFT")
XBRL_RULE = {
    "type": "next_trading_session_time_after_timestamp",
    "base": "filing.accepted_at",
    "trading_days_offset": 1,
    "time": "09:30",
    "timezone": "America/New_York",
    "calendar": "NYSE",
}


def test_ingest_captures_companyfacts_raw_and_records_policy_run() -> None:
    connection = FakeConnection()
    client = _client(connection, _responses(SEED_TICKERS))

    result = ingest_sec_companyfacts(
        connection=connection,
        client=client,
        universe="falsifier_seed",
        tickers=None,
        limit=None,
        code_git_sha="abc1234",
        sleep_seconds=0,
    )

    assert result.tickers == SEED_TICKERS
    assert result.raw_responses_captured == 2
    assert result.run_id == 1
    assert result.policy_name == "xbrl_companyfacts"
    assert result.policy_version == 1
    assert len(connection.raw_objects) == 2
    assert connection.analytics_runs[0]["status"] == "succeeded"
    assert connection.analytics_runs[0]["run_kind"] == "sec_companyfacts_ingest"
    assert connection.analytics_runs[0]["available_at_policy_versions"] == {
        "xbrl_companyfacts": 1
    }

    for ticker in SEED_TICKERS:
        raw_index = connection.events.index(f"raw:{connection.ciks[ticker]}")
        assert raw_index < connection.events.index("run:succeeded")
        assert "commit" in connection.events[
            connection.events.index("run:create") : raw_index
        ]


def test_dry_run_reads_persisted_universe_without_fetching_or_writing() -> None:
    connection = FakeConnection()

    result = ingest_sec_companyfacts(
        connection=connection,
        client=None,
        universe="falsifier_seed",
        tickers=("AAPL",),
        limit=1,
        code_git_sha="abc1234",
        dry_run=True,
    )

    assert result.dry_run is True
    assert result.tickers == ("AAPL",)
    assert connection.raw_objects == []
    assert connection.analytics_runs == []


def test_http_failure_keeps_raw_capture_and_marks_run_failed() -> None:
    connection = FakeConnection(tickers=("AAPL",))
    client = _client(
        connection,
        [
            SECTransportResponse(
                status_code=404,
                body=b'{"error":"missing"}',
                headers={"Content-Type": "application/json"},
            )
        ],
    )

    with pytest.raises(SECHTTPError, match="HTTP 404"):
        ingest_sec_companyfacts(
            connection=connection,
            client=client,
            universe="falsifier_seed",
            code_git_sha="abc1234",
            sleep_seconds=0,
        )

    assert len(connection.raw_objects) == 1
    assert connection.raw_objects[0]["http_status"] == 404
    assert connection.analytics_runs[0]["status"] == "failed"
    raw_index = connection.events.index("raw:0000320193")
    failed_index = connection.events.index("run:failed")
    assert "commit" in connection.events[raw_index:failed_index]


def test_missing_cik_fails_before_run_creation() -> None:
    connection = FakeConnection(tickers=("AAPL",))
    connection.ciks["AAPL"] = None

    with pytest.raises(SecCompanyFactsIngestError, match="missing a SEC CIK"):
        ingest_sec_companyfacts(
            connection=connection,
            client=None,
            universe="falsifier_seed",
            code_git_sha="abc1234",
            dry_run=True,
        )

    assert connection.analytics_runs == []


def test_check_config_uses_seed_reference_data() -> None:
    message = cli.check_config(
        universe="falsifier_seed",
        tickers=("AAPL", "MSFT"),
        limit=None,
        seed_config_path=cli.DEFAULT_CONFIG_PATH,
    )

    assert message == (
        "OK: checked SEC companyfacts ingest config for falsifier_seed "
        "with 2 seed ticker(s): AAPL, MSFT"
    )


def test_missing_user_agent_fails_before_database_connection(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_url = "postgresql://user:password@localhost:5432/silver"
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)

    code = cli.main(["--database-url", database_url, "--ticker", "AAPL"])

    assert code == 1
    captured = capsys.readouterr()
    assert "SEC_USER_AGENT is required" in captured.err
    assert database_url not in captured.err


def _client(
    connection: FakeConnection,
    responses: list[SECTransportResponse],
) -> SECClient:
    return SECClient(
        user_agent="Silver Test michael@example.com",
        raw_vault=RawVault(connection),
        transport=FakeTransport(responses),
        now=lambda: datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
        sleep=lambda _seconds: None,
    )


def _responses(tickers: tuple[str, ...]) -> list[SECTransportResponse]:
    return [
        SECTransportResponse(
            status_code=200,
            body=_payload(ticker),
            headers={"Content-Type": "application/json"},
        )
        for ticker in tickers
    ]


def _payload(ticker: str) -> bytes:
    return json.dumps({"cik": FakeConnection.DEFAULT_CIKS[ticker], "facts": {}}).encode(
        "utf-8"
    )


class FakeTransport:
    def __init__(self, responses: list[SECTransportResponse]) -> None:
        self._responses = list(responses)

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        timeout: float,
    ) -> SECTransportResponse:
        if not self._responses:
            raise AssertionError(f"unexpected request: {url} {headers} {timeout}")
        return self._responses.pop(0)


class FakeConnection:
    DEFAULT_CIKS = {"AAPL": "0000320193", "MSFT": "0000789019"}

    def __init__(self, *, tickers: tuple[str, ...] = SEED_TICKERS) -> None:
        self.securities = {
            ticker: security_id
            for security_id, ticker in enumerate(tickers, start=101)
        }
        self.ciks: dict[str, str | None] = {
            ticker: self.DEFAULT_CIKS[ticker] for ticker in tickers
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
        self.policies = {
            12: {
                "id": 12,
                "name": "xbrl_companyfacts",
                "version": 1,
                "rule": XBRL_RULE,
            }
        }
        self.raw_objects: list[dict[str, Any]] = []
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
        if sql.startswith("SELECT id AS security_id"):
            self._select_ciks(params)
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
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self) -> dict[str, Any] | tuple[int] | None:
        return self._one

    def fetchall(self) -> list[dict[str, Any]]:
        return self._many

    def _select_members(self, params: dict[str, Any]) -> None:
        self._many = [
            membership
            for membership in sorted(
                self.connection.memberships,
                key=lambda item: (item["ticker"], item["valid_from"]),
            )
            if membership["universe_name"] == params["universe_name"]
        ]

    def _select_ciks(self, params: dict[str, Any]) -> None:
        security_ids = set(params["security_ids"])
        rows = []
        for ticker, security_id in self.connection.securities.items():
            if security_id in security_ids and self.connection.ciks[ticker] is not None:
                rows.append(
                    {
                        "security_id": security_id,
                        "ticker": ticker,
                        "cik": self.connection.ciks[ticker],
                    }
                )
        self._many = sorted(rows, key=lambda item: item["ticker"])

    def _select_policy(self, params: dict[str, Any]) -> None:
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
            "available_at_policy_versions": json.loads(
                params["available_at_policy_versions"]
            ),
            "parameters": json.loads(params["parameters"]),
            "input_fingerprints": json.loads(params["input_fingerprints"]),
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
        self.connection.events.append(f"raw:{row['params']['cik']}")
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
