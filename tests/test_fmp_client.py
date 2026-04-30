from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import pytest

from silver.ingest.raw_vault import REDACTED_VALUE, RawVault, request_fingerprint
from silver.sources.fmp import (
    FMPClient,
    FMPConfigurationError,
    FMPHTTPError,
    FMPTransportError,
    FMPTransportResponse,
)


def test_historical_prices_builds_stable_request_and_captures_raw_response() -> None:
    payload = b"[]\n"
    transport = FakeTransport(
        [
            FMPTransportResponse(
                status_code=200,
                body=payload,
                headers={"Content-Type": "application/json"},
            )
        ]
    )
    connection = FakeConnection()
    fetched_at = datetime(2026, 4, 28, 12, 30, tzinfo=timezone.utc)

    client = FMPClient(
        api_key="real-secret",
        raw_vault=RawVault(connection),
        transport=transport,
        timeout=12.5,
        now=lambda: fetched_at,
    )

    result = client.fetch_historical_daily_prices(
        "aapl",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )

    assert transport.calls == [
        (
            "https://financialmodelingprep.com/stable/"
            "historical-price-eod/dividend-adjusted?apikey=real-secret"
            "&from=2024-01-01&symbol=AAPL&to=2024-01-31",
            12.5,
        )
    ]
    assert result.body == payload
    assert result.http_status == 200
    assert result.content_type == "application/json"
    assert result.endpoint == "/stable/historical-price-eod/dividend-adjusted"
    assert result.fetched_at == fetched_at
    assert result.request_params == {
        "apikey": REDACTED_VALUE,
        "from": "2024-01-01",
        "symbol": "AAPL",
        "to": "2024-01-31",
    }

    [row] = connection.rows
    assert row["vendor"] == "fmp"
    assert row["endpoint"] == "/stable/historical-price-eod/dividend-adjusted"
    assert row["params"] == result.request_params
    assert row["body_raw"] == payload
    assert "real-secret" not in row["request_url"]
    assert row["metadata"] == {
        "attempt_number": 1,
        "attempt_outcome": "success",
        "audit_contract": "fmp-response-audit-v1",
        "max_attempts": 3,
        "max_retries": 2,
        "retryable": False,
        "terminal": True,
    }
    assert result.raw_vault_result.request_fingerprint == request_fingerprint(
        {
            "apikey": "another-secret",
            "from": "2024-01-01",
            "symbol": "AAPL",
            "to": "2024-01-31",
        }
    )


def test_client_reads_api_key_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FMP_API_KEY", "env-secret")
    transport = FakeTransport(
        [FMPTransportResponse(status_code=200, body=b"{}", headers={})]
    )

    client = FMPClient(
        raw_vault=RawVault(FakeConnection()),
        transport=transport,
    )

    client.fetch_historical_daily_prices(
        "MSFT",
        start_date="2024-02-01",
        end_date="2024-02-02",
    )

    [(url, _timeout)] = transport.calls
    assert "apikey=env-secret" in url


def test_missing_api_key_raises_clear_configuration_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FMP_API_KEY", raising=False)

    with pytest.raises(FMPConfigurationError, match="FMP_API_KEY"):
        FMPClient(raw_vault=RawVault(FakeConnection()), transport=FakeTransport([]))


def test_terminal_non_2xx_response_is_raw_vaulted_before_http_error() -> None:
    payload = b'{"error":"not found"}'
    transport = FakeTransport(
        [
            FMPTransportResponse(
                status_code=404,
                body=payload,
                headers={"Content-Type": "application/json"},
            )
        ]
    )
    connection = FakeConnection()
    client = FMPClient(
        api_key="real-secret",
        raw_vault=RawVault(connection),
        transport=transport,
    )

    with pytest.raises(FMPHTTPError) as exc_info:
        client.fetch_historical_daily_prices(
            "AAPL",
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

    assert exc_info.value.status_code == 404
    assert exc_info.value.endpoint == "/stable/historical-price-eod/dividend-adjusted"
    assert exc_info.value.body == payload

    [row] = connection.rows
    assert row["http_status"] == 404
    assert row["body_raw"] == payload
    assert row["content_type"] == "application/json"
    assert row["params"]["apikey"] == REDACTED_VALUE
    assert "real-secret" not in row["request_url"]
    assert row["metadata"] == {
        "attempt_number": 1,
        "attempt_outcome": "terminal_failure",
        "audit_contract": "fmp-response-audit-v1",
        "max_attempts": 3,
        "max_retries": 2,
        "retryable": False,
        "terminal": True,
    }


def test_transient_failed_attempt_is_raw_vaulted_before_retry_success() -> None:
    transport = FakeTransport(
        [
            FMPTransportResponse(status_code=503, body=b"try later", headers={}),
            FMPTransportResponse(status_code=200, body=b"{}", headers={}),
        ]
    )
    connection = FakeConnection()
    sleeps: list[float] = []

    def sleep_after_raw_capture(seconds: float) -> None:
        assert len(connection.rows) == 1
        assert connection.rows[0]["http_status"] == 503
        sleeps.append(seconds)

    client = FMPClient(
        api_key="real-secret",
        raw_vault=RawVault(connection),
        transport=transport,
        max_retries=2,
        backoff_seconds=0.25,
        sleep=sleep_after_raw_capture,
    )

    result = client.fetch_historical_daily_prices(
        "AAPL",
        start_date="2024-01-01",
        end_date="2024-01-31",
    )

    assert result.http_status == 200
    assert len(transport.calls) == 2
    assert sleeps == [0.25]
    assert [row["http_status"] for row in connection.rows] == [503, 200]
    assert [row["body_raw"] for row in connection.rows] == [b"try later", b"{}"]
    assert connection.rows[0]["metadata"] == {
        "attempt_number": 1,
        "attempt_outcome": "retry_scheduled",
        "audit_contract": "fmp-response-audit-v1",
        "max_attempts": 3,
        "max_retries": 2,
        "retryable": True,
        "terminal": False,
    }
    assert connection.rows[1]["metadata"] == {
        "attempt_number": 2,
        "attempt_outcome": "success",
        "audit_contract": "fmp-response-audit-v1",
        "max_attempts": 3,
        "max_retries": 2,
        "retryable": False,
        "terminal": True,
    }
    assert connection.rows[0]["params"]["apikey"] == REDACTED_VALUE
    assert "real-secret" not in connection.rows[0]["request_url"]
    assert result.raw_vault_result.raw_object_id == connection.rows[1]["id"]


def test_transient_status_exhaustion_raw_vaults_every_response() -> None:
    transport = FakeTransport(
        [
            FMPTransportResponse(status_code=503, body=b"try later 1", headers={}),
            FMPTransportResponse(status_code=503, body=b"try later 2", headers={}),
        ]
    )
    connection = FakeConnection()
    sleeps: list[float] = []
    client = FMPClient(
        api_key="real-secret",
        raw_vault=RawVault(connection),
        transport=transport,
        max_retries=1,
        backoff_seconds=0.5,
        sleep=sleeps.append,
    )

    with pytest.raises(FMPHTTPError) as exc_info:
        client.fetch_historical_daily_prices(
            "AAPL",
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.body == b"try later 2"
    assert sleeps == [0.5]
    assert [row["http_status"] for row in connection.rows] == [503, 503]
    assert [row["body_raw"] for row in connection.rows] == [
        b"try later 1",
        b"try later 2",
    ]
    assert [row["metadata"]["attempt_number"] for row in connection.rows] == [1, 2]
    assert [row["metadata"]["attempt_outcome"] for row in connection.rows] == [
        "retry_scheduled",
        "terminal_failure",
    ]
    assert [row["metadata"]["terminal"] for row in connection.rows] == [False, True]
    assert [row["metadata"]["retryable"] for row in connection.rows] == [True, True]


def test_malformed_transport_response_raises_explicit_error() -> None:
    transport = FakeTransport([MalformedResponse()])
    connection = FakeConnection()
    client = FMPClient(
        api_key="real-secret",
        raw_vault=RawVault(connection),
        transport=transport,
    )

    with pytest.raises(FMPTransportError, match="body"):
        client.fetch_historical_daily_prices(
            "AAPL",
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

    assert connection.rows == []


class FakeTransport:
    def __init__(self, responses: list[Any]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, float]] = []

    def get(self, url: str, *, timeout: float) -> Any:
        self.calls.append((url, timeout))
        if not self._responses:
            raise AssertionError("unexpected HTTP request")
        response = self._responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


@dataclass(frozen=True)
class MalformedResponse:
    status_code: int = 200
    body: str = "not bytes"
    headers: dict[str, str] | None = None


class FakeConnection:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self._next_id = 1

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def next_id(self) -> int:
        raw_object_id = self._next_id
        self._next_id += 1
        return raw_object_id


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self._row: tuple[int] | None = None

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: dict[str, Any]) -> None:
        if sql.startswith("INSERT INTO silver.raw_objects"):
            self._insert(params)
            return
        if sql.startswith("SELECT id"):
            self._select(params)
            return
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self) -> tuple[int] | None:
        return self._row

    def _insert(self, params: dict[str, Any]) -> None:
        existing = self._find_existing(params)
        if existing is not None:
            self._row = None
            return

        row = dict(params)
        row["id"] = self.connection.next_id()
        row["params"] = json.loads(params["params"])
        row["metadata"] = json.loads(params["metadata"])
        self.connection.rows.append(row)
        self._row = (row["id"],)

    def _select(self, params: dict[str, Any]) -> None:
        existing = self._find_existing(params)
        self._row = None if existing is None else (existing["id"],)

    def _find_existing(self, params: dict[str, Any]) -> dict[str, Any] | None:
        for row in self.connection.rows:
            if (
                row["vendor"] == params["vendor"]
                and row["endpoint"] == params["endpoint"]
                and row["params_hash"] == params["params_hash"]
                and row["raw_hash"] == params["raw_hash"]
            ):
                return row
        return None
