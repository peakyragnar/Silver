from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pytest

from silver.ingest.raw_vault import RawVault, request_fingerprint
from silver.sources.sec import (
    SECClient,
    SECConfigurationError,
    SECHTTPError,
    SECTransportError,
    SECTransportResponse,
)


def test_companyfacts_builds_stable_request_and_captures_raw_response() -> None:
    payload = b'{"cik":"0000320193","facts":{}}\n'
    transport = FakeTransport(
        [
            SECTransportResponse(
                status_code=200,
                body=payload,
                headers={"Content-Type": "application/json"},
            )
        ]
    )
    connection = FakeConnection()
    fetched_at = datetime(2026, 5, 1, 12, 30, tzinfo=timezone.utc)

    client = SECClient(
        user_agent="Silver Test michael@example.com",
        raw_vault=RawVault(connection),
        transport=transport,
        timeout=12.5,
        now=lambda: fetched_at,
    )

    result = client.fetch_companyfacts("320193")

    assert transport.calls == [
        (
            "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json",
            {
                "Accept": "application/json",
                "User-Agent": "Silver Test michael@example.com",
            },
            12.5,
        )
    ]
    assert result.body == payload
    assert result.http_status == 200
    assert result.content_type == "application/json"
    assert result.endpoint == "/api/xbrl/companyfacts/CIK0000320193.json"
    assert result.fetched_at == fetched_at
    assert result.request_params == {"cik": "0000320193"}

    [row] = connection.rows
    assert row["vendor"] == "sec"
    assert row["endpoint"] == "/api/xbrl/companyfacts/CIK0000320193.json"
    assert row["params"] == {"cik": "0000320193"}
    assert row["body_raw"] == payload
    assert row["metadata"] == {
        "attempt_number": 1,
        "attempt_outcome": "success",
        "audit_contract": "sec-companyfacts-response-audit-v1",
        "max_attempts": 3,
        "max_retries": 2,
        "retryable": False,
        "terminal": True,
        "user_agent_declared": True,
    }
    assert result.raw_vault_result.request_fingerprint == request_fingerprint(
        {"cik": "0000320193"}
    )


def test_missing_user_agent_raises_clear_configuration_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)

    with pytest.raises(SECConfigurationError, match="SEC_USER_AGENT"):
        SECClient(raw_vault=RawVault(FakeConnection()), transport=FakeTransport([]))


def test_terminal_non_2xx_response_is_raw_vaulted_before_http_error() -> None:
    payload = b'{"error":"not found"}'
    transport = FakeTransport(
        [
            SECTransportResponse(
                status_code=404,
                body=payload,
                headers={"Content-Type": "application/json"},
            )
        ]
    )
    connection = FakeConnection()
    client = SECClient(
        user_agent="Silver Test michael@example.com",
        raw_vault=RawVault(connection),
        transport=transport,
    )

    with pytest.raises(SECHTTPError) as exc_info:
        client.fetch_companyfacts("0000320193")

    assert exc_info.value.status_code == 404
    assert exc_info.value.endpoint == "/api/xbrl/companyfacts/CIK0000320193.json"
    assert exc_info.value.body == payload

    [row] = connection.rows
    assert row["http_status"] == 404
    assert row["body_raw"] == payload
    assert row["metadata"] == {
        "attempt_number": 1,
        "attempt_outcome": "terminal_failure",
        "audit_contract": "sec-companyfacts-response-audit-v1",
        "max_attempts": 3,
        "max_retries": 2,
        "retryable": False,
        "terminal": True,
        "user_agent_declared": True,
    }


def test_malformed_transport_response_raises_explicit_error() -> None:
    transport = FakeTransport([MalformedResponse()])
    connection = FakeConnection()
    client = SECClient(
        user_agent="Silver Test michael@example.com",
        raw_vault=RawVault(connection),
        transport=transport,
    )

    with pytest.raises(SECTransportError, match="body"):
        client.fetch_companyfacts("0000320193")

    assert connection.rows == []


class FakeTransport:
    def __init__(self, responses: list[Any]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, dict[str, str], float]] = []

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str],
        timeout: float,
    ) -> Any:
        self.calls.append((url, dict(headers), timeout))
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
