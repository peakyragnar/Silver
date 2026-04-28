from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from typing import Any

import pytest

from silver.ingest.raw_vault import (
    REDACTED_VALUE,
    RawVault,
    RawVaultError,
    content_hash,
    request_fingerprint,
)


def test_content_hash_uses_exact_response_bytes() -> None:
    payload = b'{"b":2,"a":1}\n'

    assert content_hash(payload) == hashlib.sha256(payload).hexdigest()
    assert content_hash(bytearray(payload)) == hashlib.sha256(payload).hexdigest()
    assert content_hash(b"") == hashlib.sha256(b"").hexdigest()


def test_request_fingerprint_is_stable_for_semantically_identical_params() -> None:
    first = {
        "symbol": "AAPL",
        "period": "annual",
        "window": {"to": date(2024, 12, 31), "from": date(2024, 1, 1)},
        "apikey": "first-secret",
    }
    second = {
        "apikey": "second-secret",
        "window": {"from": "2024-01-01", "to": "2024-12-31"},
        "period": "annual",
        "symbol": "AAPL",
    }

    assert request_fingerprint(first) == request_fingerprint(second)


def test_write_response_inserts_raw_bytes_and_metadata() -> None:
    connection = FakeConnection()
    vault = RawVault(connection)
    payload = b'{"symbol":"AAPL","price":196.58}\n'
    fetched_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    metadata = {"cache_path": "fixtures/fmp/aapl.json", "attempt": 1}

    result = vault.write_response(
        source="fmp",
        endpoint="/api/v3/profile/AAPL",
        params={"symbol": "AAPL", "apikey": "secret-value"},
        request_url=(
            "https://financialmodelingprep.com/api/v3/profile/AAPL"
            "?apikey=secret-value&symbol=AAPL"
        ),
        body=payload,
        http_status=200,
        content_type="application/json",
        fetched_at=fetched_at,
        metadata=metadata,
    )

    assert result.raw_object_id == 1
    assert result.source == "fmp"
    assert result.endpoint == "/api/v3/profile/AAPL"
    assert result.inserted is True
    assert result.content_hash == hashlib.sha256(payload).hexdigest()

    [row] = connection.rows
    assert row["vendor"] == "fmp"
    assert row["endpoint"] == "/api/v3/profile/AAPL"
    assert row["params"] == {"symbol": "AAPL", "apikey": REDACTED_VALUE}
    assert "secret-value" not in row["request_url"]
    assert row["body_raw"] == payload
    assert row["raw_hash"] == result.content_hash
    assert row["fetched_at"] == fetched_at
    assert row["metadata"] == metadata


def test_write_response_is_idempotent_without_mutating_existing_metadata() -> None:
    connection = FakeConnection()
    vault = RawVault(connection)
    fetched_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    first = vault.write_response(
        source="sec",
        endpoint="/Archives/edgar/data/1/index.json",
        params={"cik": "0000000001"},
        body=b"first payload",
        http_status=200,
        fetched_at=fetched_at,
        metadata={"attempt": 1},
    )
    second = vault.write_response(
        source="sec",
        endpoint="/Archives/edgar/data/1/index.json",
        params={"cik": "0000000001"},
        body=b"first payload",
        http_status=200,
        fetched_at=fetched_at,
        metadata={"attempt": 2},
    )

    assert first.raw_object_id == second.raw_object_id == 1
    assert first.inserted is True
    assert second.inserted is False
    assert len(connection.rows) == 1
    assert connection.rows[0]["metadata"] == {"attempt": 1}


def test_changed_payload_appends_new_raw_object_for_same_request() -> None:
    connection = FakeConnection()
    vault = RawVault(connection)
    fetched_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    first = vault.write_response(
        source="fmp",
        endpoint="/api/v3/historical-price-full/AAPL",
        params={"symbol": "AAPL"},
        body=b"first payload",
        http_status=200,
        fetched_at=fetched_at,
    )
    second = vault.write_response(
        source="fmp",
        endpoint="/api/v3/historical-price-full/AAPL",
        params={"symbol": "AAPL"},
        body=b"changed payload",
        http_status=200,
        fetched_at=fetched_at,
    )

    assert first.raw_object_id == 1
    assert second.raw_object_id == 2
    assert first.content_hash != second.content_hash
    assert len(connection.rows) == 2


@pytest.mark.parametrize(
    ("override", "error"),
    (
        ({"source": ""}, "source must be a non-empty string"),
        ({"endpoint": "   "}, "endpoint must be a non-empty string"),
        ({"body": "not bytes"}, "body must be bytes"),
        ({"http_status": 99}, "http_status must be between"),
        ({"params": ["symbol", "AAPL"]}, "params must be a mapping"),
        ({"metadata": ["attempt", 1]}, "metadata must be a mapping"),
        ({"fetched_at": datetime(2026, 1, 2, 3, 4, 5)}, "timezone-aware"),
    ),
)
def test_write_response_rejects_invalid_inputs(
    override: dict[str, Any],
    error: str,
) -> None:
    kwargs = {
        "source": "fmp",
        "endpoint": "/api/v3/profile/AAPL",
        "params": {"symbol": "AAPL"},
        "body": b"payload",
        "http_status": 200,
        "fetched_at": datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
    }
    kwargs.update(override)

    with pytest.raises(RawVaultError, match=error):
        RawVault(FakeConnection()).write_response(**kwargs)


class FakeConnection:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.executed: list[tuple[str, dict[str, Any]]] = []
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
        self.connection.executed.append((sql, dict(params)))
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
