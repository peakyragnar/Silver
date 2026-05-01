"""SEC EDGAR client that captures companyfacts responses before parsing."""

from __future__ import annotations

import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from silver.ingest.raw_vault import RawVaultWriteResult


SEC_SOURCE = "sec"
SEC_COMPANYFACTS_AUDIT_CONTRACT = "sec-companyfacts-response-audit-v1"
DEFAULT_BASE_URL = "https://data.sec.gov"
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_RETRIES = 2
DEFAULT_BACKOFF_SECONDS = 0.5
TRANSIENT_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


class SECClientError(RuntimeError):
    """Base error for SEC client failures."""


class SECConfigurationError(SECClientError):
    """Raised when the SEC client is missing required configuration."""


class SECTransportError(SECClientError):
    """Raised when HTTP transport does not produce a usable response."""


class SECHTTPError(SECClientError):
    """Raised for non-successful SEC HTTP responses."""

    def __init__(self, *, endpoint: str, status_code: int, body: bytes) -> None:
        self.endpoint = endpoint
        self.status_code = status_code
        self.body = body
        super().__init__(
            f"SEC request failed with HTTP {status_code} for endpoint {endpoint}"
        )


class SECTransport(Protocol):
    """Minimal HTTP transport boundary for deterministic SEC client tests."""

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout: float,
    ) -> Any:
        """Return an HTTP response-like object for ``url``."""


@dataclass(frozen=True)
class SECTransportResponse:
    """HTTP response shape used by the default and fake transports."""

    status_code: int
    body: bytes | bytearray | memoryview
    headers: Mapping[str, str] | None = None


@dataclass(frozen=True)
class SECRawResponse:
    """Exact SEC response bytes plus request/capture metadata."""

    source: str
    endpoint: str
    request_url: str
    request_params: dict[str, str]
    body: bytes
    http_status: int
    content_type: str | None
    fetched_at: datetime
    raw_vault_result: RawVaultWriteResult


@dataclass(frozen=True)
class _SECRequest:
    endpoint: str
    request_url: str
    vault_params: dict[str, str]


class SECClient:
    """Small SEC client for raw companyfacts capture through ``RawVault``."""

    def __init__(
        self,
        *,
        raw_vault: Any,
        user_agent: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        transport: SECTransport | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
        sleep: Callable[[float], Any] = time.sleep,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._user_agent = _user_agent(user_agent)
        self._raw_vault = raw_vault
        self._base_url = _base_url(base_url)
        self._transport = transport or UrllibSECTransport()
        self._timeout = _positive_number(timeout, "timeout")
        self._max_retries = _max_retries(max_retries)
        self._backoff_seconds = _non_negative_number(
            backoff_seconds,
            "backoff_seconds",
        )
        self._sleep = sleep
        self._now = now or _utc_now

    def fetch_companyfacts(self, cik: str | int) -> SECRawResponse:
        """Fetch SEC companyfacts JSON for one CIK and raw-vault exact bytes."""
        request = self._companyfacts_request(cik)
        return self._get_with_retries(request)

    def _companyfacts_request(self, cik: str | int) -> _SECRequest:
        normalized_cik = _cik(cik)
        endpoint = f"/api/xbrl/companyfacts/CIK{normalized_cik}.json"
        return _SECRequest(
            endpoint=endpoint,
            request_url=f"{self._base_url}{endpoint}",
            vault_params={"cik": normalized_cik},
        )

    def _get_with_retries(self, request: _SECRequest) -> SECRawResponse:
        max_attempts = self._max_retries + 1
        for attempt in range(1, max_attempts + 1):
            response = self._get_once(request)

            success = _success_status(response.status_code)
            retryable = response.status_code in TRANSIENT_HTTP_STATUSES
            retry_scheduled = retryable and not success and attempt <= self._max_retries
            terminal = success or not retry_scheduled
            if success:
                attempt_outcome = "success"
            elif retry_scheduled:
                attempt_outcome = "retry_scheduled"
            else:
                attempt_outcome = "terminal_failure"

            raw_response = self._write_raw_response(
                request=request,
                response=response,
                metadata={
                    "audit_contract": SEC_COMPANYFACTS_AUDIT_CONTRACT,
                    "attempt_number": attempt,
                    "max_retries": self._max_retries,
                    "max_attempts": max_attempts,
                    "retryable": retryable,
                    "terminal": terminal,
                    "attempt_outcome": attempt_outcome,
                    "user_agent_declared": True,
                },
            )

            if success:
                return raw_response
            if retry_scheduled:
                self._sleep(self._retry_delay(attempt))
                continue
            raise SECHTTPError(
                endpoint=request.endpoint,
                status_code=response.status_code,
                body=response.body,
            )

        raise AssertionError("retry loop exhausted without returning or raising")

    def _get_once(self, request: _SECRequest) -> SECTransportResponse:
        try:
            response = self._transport.get(
                request.request_url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": self._user_agent,
                },
                timeout=self._timeout,
            )
        except SECTransportError:
            raise
        except Exception as exc:  # noqa: BLE001 - transport boundary is pluggable.
            raise SECTransportError(
                f"SEC transport failed for endpoint {request.endpoint}: "
                f"{type(exc).__name__}"
            ) from exc
        return _response(response)

    def _write_raw_response(
        self,
        *,
        request: _SECRequest,
        response: SECTransportResponse,
        metadata: Mapping[str, Any],
    ) -> SECRawResponse:
        fetched_at = self._now()
        if fetched_at.tzinfo is None or fetched_at.utcoffset() is None:
            raise SECTransportError("now() must return a timezone-aware datetime")

        content_type = _content_type(response.headers)
        vault_result = self._raw_vault.write_response(
            source=SEC_SOURCE,
            endpoint=request.endpoint,
            params=request.vault_params,
            request_url=request.request_url,
            body=response.body,
            http_status=response.status_code,
            content_type=content_type,
            fetched_at=fetched_at,
            metadata=metadata,
        )

        return SECRawResponse(
            source=SEC_SOURCE,
            endpoint=request.endpoint,
            request_url=request.request_url,
            request_params=dict(request.vault_params),
            body=response.body,
            http_status=response.status_code,
            content_type=content_type,
            fetched_at=fetched_at,
            raw_vault_result=vault_result,
        )

    def _retry_delay(self, attempt: int) -> float:
        return self._backoff_seconds * (2 ** (attempt - 1))


class UrllibSECTransport:
    """Default stdlib transport for SEC HTTP GET requests."""

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout: float,
    ) -> SECTransportResponse:
        request = urllib.request.Request(url, headers=dict(headers), method="GET")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return SECTransportResponse(
                    status_code=int(response.status),
                    body=response.read(),
                    headers=dict(response.headers.items()),
                )
        except urllib.error.HTTPError as exc:
            return SECTransportResponse(
                status_code=int(exc.code),
                body=exc.read(),
                headers=dict(exc.headers.items()),
            )
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            raise SECTransportError(f"SEC transport failed: {exc}") from exc


def _user_agent(value: str | None) -> str:
    configured = os.environ.get("SEC_USER_AGENT") if value is None else value
    if configured is None or not configured.strip():
        raise SECConfigurationError(
            "SEC user agent is required; pass user_agent or set SEC_USER_AGENT "
            "to a descriptive value such as 'Company Name contact@example.com'"
        )
    return configured.strip()


def _base_url(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SECConfigurationError("base_url must be a non-empty string")
    normalized = value.strip().rstrip("/")
    parts = urllib.parse.urlsplit(normalized)
    if not parts.scheme or not parts.netloc:
        raise SECConfigurationError("base_url must include scheme and host")
    return normalized


def _cik(value: str | int) -> str:
    if isinstance(value, bool):
        raise SECConfigurationError("cik must be a CIK string or integer")
    if isinstance(value, int):
        if value <= 0:
            raise SECConfigurationError("cik must be positive")
        raw = str(value)
    elif isinstance(value, str):
        raw = value.strip()
    else:
        raise SECConfigurationError("cik must be a CIK string or integer")
    if not raw.isdigit() or len(raw) > 10:
        raise SECConfigurationError("cik must contain 1 to 10 digits")
    return raw.zfill(10)


def _positive_number(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SECConfigurationError(f"{name} must be a positive number")
    normalized = float(value)
    if normalized <= 0:
        raise SECConfigurationError(f"{name} must be a positive number")
    return normalized


def _non_negative_number(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SECConfigurationError(f"{name} must be non-negative")
    normalized = float(value)
    if normalized < 0:
        raise SECConfigurationError(f"{name} must be non-negative")
    return normalized


def _max_retries(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SECConfigurationError("max_retries must be a non-negative integer")
    return value


def _success_status(status_code: int) -> bool:
    return 200 <= status_code <= 299


def _response(value: Any) -> SECTransportResponse:
    status_code = getattr(value, "status_code", None)
    if isinstance(status_code, bool) or not isinstance(status_code, int):
        raise SECTransportError("SEC transport response status_code must be an integer")
    if status_code < 100 or status_code > 599:
        raise SECTransportError("SEC transport response status_code must be 100-599")

    body = getattr(value, "body", None)
    if not isinstance(body, (bytes, bytearray, memoryview)):
        raise SECTransportError("SEC transport response body must be bytes-like")

    headers = getattr(value, "headers", None)
    if headers is None:
        normalized_headers: dict[str, str] = {}
    elif isinstance(headers, Mapping):
        normalized_headers = {str(key): str(item) for key, item in headers.items()}
    else:
        raise SECTransportError("SEC transport response headers must be a mapping")

    return SECTransportResponse(
        status_code=status_code,
        body=bytes(body),
        headers=normalized_headers,
    )


def _content_type(headers: Mapping[str, str] | None) -> str | None:
    if headers is None:
        return None
    for key, item in headers.items():
        if key.lower() == "content-type":
            return item
    return None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


__all__ = [
    "SECClient",
    "SECClientError",
    "SECConfigurationError",
    "SECHTTPError",
    "SECRawResponse",
    "SECTransport",
    "SECTransportError",
    "SECTransportResponse",
]
