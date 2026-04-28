"""Financial Modeling Prep client that captures raw responses before parsing."""

from __future__ import annotations

import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Protocol

from silver.ingest.raw_vault import REDACTED_VALUE, RawVaultWriteResult


FMP_SOURCE = "fmp"
DEFAULT_BASE_URL = "https://financialmodelingprep.com"
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_RETRIES = 2
DEFAULT_BACKOFF_SECONDS = 0.5
TRANSIENT_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


class FMPClientError(RuntimeError):
    """Base error for FMP client failures."""


class FMPConfigurationError(FMPClientError):
    """Raised when the FMP client is missing required configuration."""


class FMPTransportError(FMPClientError):
    """Raised when HTTP transport does not produce a usable response."""


class FMPHTTPError(FMPClientError):
    """Raised for non-successful FMP HTTP responses."""

    def __init__(self, *, endpoint: str, status_code: int, body: bytes) -> None:
        self.endpoint = endpoint
        self.status_code = status_code
        self.body = body
        super().__init__(
            f"FMP request failed with HTTP {status_code} for endpoint {endpoint}"
        )


class FMPTransport(Protocol):
    """Minimal HTTP transport boundary for deterministic source-client tests."""

    def get(self, url: str, *, timeout: float) -> Any:
        """Return an HTTP response-like object for ``url``."""


@dataclass(frozen=True)
class FMPTransportResponse:
    """HTTP response shape used by the default and fake transports."""

    status_code: int
    body: bytes | bytearray | memoryview
    headers: Mapping[str, str] | None = None


@dataclass(frozen=True)
class FMPRawResponse:
    """Exact FMP response bytes plus request/capture metadata."""

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
class _FMPRequest:
    endpoint: str
    request_url: str
    safe_request_url: str
    vault_params: dict[str, str]
    safe_vault_params: dict[str, str]


class FMPClient:
    """Small FMP client for raw source capture through ``RawVault``."""

    def __init__(
        self,
        *,
        raw_vault: Any,
        api_key: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        transport: FMPTransport | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
        sleep: Callable[[float], Any] = time.sleep,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._api_key = _api_key(api_key)
        self._raw_vault = raw_vault
        self._base_url = _base_url(base_url)
        self._transport = transport or UrllibFMPTransport()
        self._timeout = _positive_number(timeout, "timeout")
        self._max_retries = _max_retries(max_retries)
        self._backoff_seconds = _non_negative_number(
            backoff_seconds,
            "backoff_seconds",
        )
        self._sleep = sleep
        self._now = now or _utc_now

    def fetch_historical_daily_prices(
        self,
        symbol: str,
        *,
        start_date: date | str,
        end_date: date | str,
    ) -> FMPRawResponse:
        """Fetch FMP historical daily prices and persist the exact response bytes."""
        request = self._historical_daily_price_request(symbol, start_date, end_date)
        response, attempt = self._get_with_retries(request)
        fetched_at = self._now()
        if fetched_at.tzinfo is None or fetched_at.utcoffset() is None:
            raise FMPTransportError("now() must return a timezone-aware datetime")

        vault_result = self._raw_vault.write_response(
            source=FMP_SOURCE,
            endpoint=request.endpoint,
            params=request.vault_params,
            request_url=request.request_url,
            body=response.body,
            http_status=response.status_code,
            content_type=_content_type(response.headers),
            fetched_at=fetched_at,
            metadata={"attempt": attempt, "max_retries": self._max_retries},
        )

        return FMPRawResponse(
            source=FMP_SOURCE,
            endpoint=request.endpoint,
            request_url=request.safe_request_url,
            request_params=dict(request.safe_vault_params),
            body=response.body,
            http_status=response.status_code,
            content_type=_content_type(response.headers),
            fetched_at=fetched_at,
            raw_vault_result=vault_result,
        )

    def _historical_daily_price_request(
        self,
        symbol: str,
        start_date: date | str,
        end_date: date | str,
    ) -> _FMPRequest:
        normalized_symbol = _symbol(symbol)
        start = _request_date(start_date, "start_date")
        end = _request_date(end_date, "end_date")
        if start > end:
            raise FMPConfigurationError("start_date must be on or before end_date")

        endpoint = (
            "/api/v3/historical-price-full/"
            f"{urllib.parse.quote(normalized_symbol, safe='')}"
        )
        query_params = {
            "apikey": self._api_key,
            "from": start.isoformat(),
            "to": end.isoformat(),
        }
        vault_params = {
            "apikey": self._api_key,
            "from": start.isoformat(),
            "symbol": normalized_symbol,
            "to": end.isoformat(),
        }
        request_url = _url(self._base_url, endpoint, query_params)
        return _FMPRequest(
            endpoint=endpoint,
            request_url=request_url,
            safe_request_url=_redacted_url(request_url),
            vault_params=vault_params,
            safe_vault_params=_redacted_params(vault_params),
        )

    def _get_with_retries(
        self,
        request: _FMPRequest,
    ) -> tuple[FMPTransportResponse, int]:
        for attempt in range(1, self._max_retries + 2):
            response = self._get_once(request)
            if 200 <= response.status_code <= 299:
                return response, attempt
            if (
                response.status_code in TRANSIENT_HTTP_STATUSES
                and attempt <= self._max_retries
            ):
                self._sleep(self._retry_delay(attempt))
                continue
            raise FMPHTTPError(
                endpoint=request.endpoint,
                status_code=response.status_code,
                body=response.body,
            )

        raise AssertionError("retry loop exhausted without returning or raising")

    def _get_once(self, request: _FMPRequest) -> FMPTransportResponse:
        try:
            response = self._transport.get(
                request.request_url,
                timeout=self._timeout,
            )
        except FMPTransportError:
            raise
        except Exception as exc:  # noqa: BLE001 - transport boundary is pluggable.
            raise FMPTransportError(
                f"FMP transport failed for endpoint {request.endpoint}: "
                f"{type(exc).__name__}"
            ) from exc
        return _response(response)

    def _retry_delay(self, attempt: int) -> float:
        return self._backoff_seconds * (2 ** (attempt - 1))


class UrllibFMPTransport:
    """Default stdlib transport for FMP HTTP GET requests."""

    def get(self, url: str, *, timeout: float) -> FMPTransportResponse:
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "Silver/0.1 FMP raw client"},
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return FMPTransportResponse(
                    status_code=int(response.status),
                    body=response.read(),
                    headers=dict(response.headers.items()),
                )
        except urllib.error.HTTPError as exc:
            return FMPTransportResponse(
                status_code=int(exc.code),
                body=exc.read(),
                headers=dict(exc.headers.items()),
            )
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            raise FMPTransportError(f"FMP transport failed: {exc}") from exc


def _api_key(value: str | None) -> str:
    configured = os.environ.get("FMP_API_KEY") if value is None else value
    if configured is None or not configured.strip():
        raise FMPConfigurationError(
            "FMP API key is required; pass api_key or set FMP_API_KEY"
        )
    return configured.strip()


def _base_url(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FMPConfigurationError("base_url must be a non-empty string")
    normalized = value.strip().rstrip("/")
    parts = urllib.parse.urlsplit(normalized)
    if not parts.scheme or not parts.netloc:
        raise FMPConfigurationError("base_url must include scheme and host")
    return normalized


def _positive_number(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FMPConfigurationError(f"{name} must be a positive number")
    normalized = float(value)
    if normalized <= 0:
        raise FMPConfigurationError(f"{name} must be a positive number")
    return normalized


def _non_negative_number(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FMPConfigurationError(f"{name} must be non-negative")
    normalized = float(value)
    if normalized < 0:
        raise FMPConfigurationError(f"{name} must be non-negative")
    return normalized


def _max_retries(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FMPConfigurationError("max_retries must be a non-negative integer")
    return value


def _symbol(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FMPConfigurationError("symbol must be a non-empty string")
    return value.strip().upper()


def _request_date(value: date | str, name: str) -> date:
    if isinstance(value, datetime):
        raise FMPConfigurationError(f"{name} must be a date, not a datetime")
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise FMPConfigurationError(
                f"{name} must be an ISO date in YYYY-MM-DD format"
            ) from exc
    raise FMPConfigurationError(f"{name} must be a date or ISO date string")


def _url(base_url: str, endpoint: str, query_params: Mapping[str, str]) -> str:
    query = urllib.parse.urlencode(sorted(query_params.items()))
    return f"{base_url}{endpoint}?{query}"


def _redacted_params(params: Mapping[str, str]) -> dict[str, str]:
    return {
        key: REDACTED_VALUE if key.lower().replace("_", "-") == "apikey" else value
        for key, value in params.items()
    }


def _redacted_url(value: str) -> str:
    parts = urllib.parse.urlsplit(value)
    query_items = []
    for key, item in urllib.parse.parse_qsl(parts.query, keep_blank_values=True):
        safe_item = REDACTED_VALUE if key.lower().replace("_", "-") == "apikey" else item
        query_items.append((key, safe_item))
    return urllib.parse.urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urllib.parse.urlencode(sorted(query_items)),
            parts.fragment,
        )
    )


def _response(value: Any) -> FMPTransportResponse:
    status_code = getattr(value, "status_code", None)
    if isinstance(status_code, bool) or not isinstance(status_code, int):
        raise FMPTransportError("FMP transport response status_code must be an integer")
    if status_code < 100 or status_code > 599:
        raise FMPTransportError("FMP transport response status_code must be 100-599")

    body = getattr(value, "body", None)
    if not isinstance(body, (bytes, bytearray, memoryview)):
        raise FMPTransportError("FMP transport response body must be bytes-like")

    headers = getattr(value, "headers", None)
    if headers is None:
        normalized_headers: dict[str, str] = {}
    elif isinstance(headers, Mapping):
        normalized_headers = {str(key): str(item) for key, item in headers.items()}
    else:
        raise FMPTransportError("FMP transport response headers must be a mapping")

    return FMPTransportResponse(
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
    "FMPClient",
    "FMPClientError",
    "FMPConfigurationError",
    "FMPHTTPError",
    "FMPRawResponse",
    "FMPTransport",
    "FMPTransportError",
    "FMPTransportResponse",
]
