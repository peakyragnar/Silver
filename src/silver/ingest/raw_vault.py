"""Append-only writer for exact source responses in ``silver.raw_objects``."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


SECRET_FIELD_NAMES = frozenset(
    {
        "access_token",
        "api-key",
        "api_key",
        "apikey",
        "authorization",
        "client_secret",
        "password",
        "secret",
        "token",
    }
)
NORMALIZED_SECRET_FIELD_NAMES = frozenset(
    name.replace("_", "-") for name in SECRET_FIELD_NAMES
)
REDACTED_VALUE = "[REDACTED]"


class RawVaultError(ValueError):
    """Raised when a raw vault write request is invalid."""


@dataclass(frozen=True)
class RawVaultWriteResult:
    raw_object_id: int
    source: str
    endpoint: str
    request_fingerprint: str
    content_hash: str
    inserted: bool


class RawVault:
    """Write raw source response bodies into ``silver.raw_objects``.

    The writer intentionally does not commit. Callers own transaction boundaries
    so raw capture can be grouped with later ingest steps when appropriate.
    """

    def __init__(self, connection: Any):
        self._connection = connection

    def write_response(
        self,
        *,
        source: str,
        endpoint: str,
        body: bytes | bytearray | memoryview,
        http_status: int,
        params: Mapping[str, Any] | None = None,
        request_url: str | None = None,
        content_type: str | None = None,
        fetched_at: datetime | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> RawVaultWriteResult:
        """Persist one raw response body, returning the inserted or existing row id."""
        normalized_source = _required_label(source, "source")
        normalized_endpoint = _required_label(endpoint, "endpoint")
        body_bytes = _body_bytes(body)
        normalized_status = _http_status(http_status)
        normalized_params = _request_params(params)
        normalized_metadata = _metadata(metadata)
        normalized_request_url = _request_url(request_url, normalized_endpoint)
        normalized_content_type = _optional_label(content_type, "content_type")
        normalized_fetched_at = _fetched_at(fetched_at)

        params_json = _stable_json(normalized_params)
        metadata_json = _stable_json(normalized_metadata)
        params_hash = _sha256_text(params_json)
        raw_hash = content_hash(body_bytes)

        insert_params = {
            "vendor": normalized_source,
            "endpoint": normalized_endpoint,
            "params_hash": params_hash,
            "params": params_json,
            "request_url": normalized_request_url,
            "http_status": normalized_status,
            "content_type": normalized_content_type,
            "body_raw": body_bytes,
            "raw_hash": raw_hash,
            "fetched_at": normalized_fetched_at,
            "metadata": metadata_json,
        }

        with _cursor(self._connection) as cursor:
            cursor.execute(_INSERT_SQL, insert_params)
            row = cursor.fetchone()
        if row is not None:
            raw_object_id = _row_id(row)
            return RawVaultWriteResult(
                raw_object_id=raw_object_id,
                source=normalized_source,
                endpoint=normalized_endpoint,
                request_fingerprint=params_hash,
                content_hash=raw_hash,
                inserted=True,
            )

        lookup_params = {
            "vendor": normalized_source,
            "endpoint": normalized_endpoint,
            "params_hash": params_hash,
            "raw_hash": raw_hash,
        }
        with _cursor(self._connection) as cursor:
            cursor.execute(_SELECT_EXISTING_SQL, lookup_params)
            row = cursor.fetchone()
        if row is None:
            raise RawVaultError("raw object insert conflicted but existing row was not found")

        return RawVaultWriteResult(
            raw_object_id=_row_id(row),
            source=normalized_source,
            endpoint=normalized_endpoint,
            request_fingerprint=params_hash,
            content_hash=raw_hash,
            inserted=False,
        )


def content_hash(body: bytes | bytearray | memoryview) -> str:
    """Return the SHA-256 hash of exact response bytes."""
    return hashlib.sha256(_body_bytes(body)).hexdigest()


def request_fingerprint(params: Mapping[str, Any] | None = None) -> str:
    """Return the stable SHA-256 fingerprint for sanitized request inputs."""
    return _sha256_text(_stable_json(_request_params(params)))


def _body_bytes(body: bytes | bytearray | memoryview) -> bytes:
    if not isinstance(body, (bytes, bytearray, memoryview)):
        raise RawVaultError("body must be bytes, bytearray, or memoryview")
    return bytes(body)


def _required_label(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RawVaultError(f"{name} must be a non-empty string")
    return value.strip()


def _optional_label(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _required_label(value, name)


def _http_status(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RawVaultError("http_status must be an integer")
    if value < 100 or value > 599:
        raise RawVaultError("http_status must be between 100 and 599")
    return value


def _request_params(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise RawVaultError("params must be a mapping")
    return _redact_secrets(value)


def _metadata(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise RawVaultError("metadata must be a mapping")
    return dict(value)


def _request_url(value: str | None, endpoint: str) -> str:
    if value is None:
        return _sanitize_url(endpoint)
    if not isinstance(value, str) or not value.strip():
        raise RawVaultError("request_url must be a non-empty string when provided")
    return _sanitize_url(value.strip())


def _fetched_at(value: datetime | None) -> datetime:
    fetched_at = value or datetime.now(timezone.utc)
    if not isinstance(fetched_at, datetime):
        raise RawVaultError("fetched_at must be a datetime")
    if fetched_at.tzinfo is None or fetched_at.utcoffset() is None:
        raise RawVaultError("fetched_at must be timezone-aware")
    return fetched_at


def _redact_secrets(value: Any) -> Any:
    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise RawVaultError("request parameter keys must be non-empty strings")
            if _is_secret_field(key):
                redacted[key] = REDACTED_VALUE
            else:
                redacted[key] = _redact_secrets(item)
        return redacted
    if isinstance(value, tuple):
        return [_redact_secrets(item) for item in value]
    if isinstance(value, list):
        return [_redact_secrets(item) for item in value]
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise RawVaultError("datetime request parameters must be timezone-aware")
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _stable_json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=_reject_json)
    except TypeError as exc:
        raise RawVaultError("value must be JSON serializable") from exc


def _reject_json(value: object) -> object:
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sanitize_url(value: str) -> str:
    parts = urlsplit(value)
    query_items = []
    for key, item in parse_qsl(parts.query, keep_blank_values=True):
        safe_item = REDACTED_VALUE if _is_secret_field(key) else item
        query_items.append((key, safe_item))
    query = urlencode(sorted(query_items), doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def _is_secret_field(name: str) -> bool:
    return name.strip().lower().replace("_", "-") in NORMALIZED_SECRET_FIELD_NAMES


def _row_id(row: object) -> int:
    if isinstance(row, Mapping):
        value = row.get("id")
    else:
        value = row[0]  # type: ignore[index]
    if isinstance(value, bool) or not isinstance(value, int):
        raise RawVaultError("raw_objects.id returned by database must be an integer")
    return value


@contextmanager
def _cursor(connection: Any) -> Any:
    cursor = connection.cursor()
    if hasattr(cursor, "__enter__"):
        with cursor as managed_cursor:
            yield managed_cursor
        return
    try:
        yield cursor
    finally:
        close = getattr(cursor, "close", None)
        if close is not None:
            close()


_INSERT_SQL = """
INSERT INTO silver.raw_objects (
    vendor,
    endpoint,
    params_hash,
    params,
    request_url,
    http_status,
    content_type,
    body_jsonb,
    body_raw,
    raw_hash,
    fetched_at,
    metadata
) VALUES (
    %(vendor)s,
    %(endpoint)s,
    %(params_hash)s,
    %(params)s::jsonb,
    %(request_url)s,
    %(http_status)s,
    %(content_type)s,
    NULL,
    %(body_raw)s,
    %(raw_hash)s,
    %(fetched_at)s,
    %(metadata)s::jsonb
)
ON CONFLICT (vendor, endpoint, params_hash, raw_hash) DO NOTHING
RETURNING id;
""".strip()

_SELECT_EXISTING_SQL = """
SELECT id
FROM silver.raw_objects
WHERE vendor = %(vendor)s
  AND endpoint = %(endpoint)s
  AND params_hash = %(params_hash)s
  AND raw_hash = %(raw_hash)s
LIMIT 1;
""".strip()
