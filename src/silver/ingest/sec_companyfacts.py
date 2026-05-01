"""Orchestrate SEC companyfacts ingest through the raw vault."""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any

from silver.analytics import AnalyticsRunRepository
from silver.reference import UniverseMember, UniverseMembershipRepository
from silver.sources.sec import SECClient, SECHTTPError


SEC_COMPANYFACTS_RUN_KIND = "sec_companyfacts_ingest"
SEC_COMPANYFACTS_POLICY_NAME = "xbrl_companyfacts"
SEC_COMPANYFACTS_POLICY_VERSION = 1
SEC_COMPANYFACTS_ENDPOINT_TEMPLATE = "/api/xbrl/companyfacts/CIK##########.json"
SEC_COMPANYFACTS_AUDIT_CONTRACT = "sec-companyfacts-raw-ingest-v1"
CIK_RE = re.compile(r"^\d{10}$")


class SecCompanyFactsIngestError(RuntimeError):
    """Raised when SEC companyfacts ingest cannot complete safely."""


@dataclass(frozen=True, slots=True)
class SecCompanyFactsPolicy:
    """Available-at policy metadata used by future companyfacts normalization."""

    id: int
    name: str
    version: int
    rule: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class SecCompanyFactsMember:
    """One universe security with the SEC CIK needed for companyfacts."""

    security_id: int
    ticker: str
    cik: str
    universe_name: str
    valid_from: date
    valid_to: date | None


@dataclass(frozen=True, slots=True)
class SecCompanyFactsTickerResult:
    """Per-security raw-ingest summary."""

    ticker: str
    cik: str
    raw_object_id: int
    raw_inserted: bool
    http_status: int
    bytes_captured: int


@dataclass(frozen=True, slots=True)
class SecCompanyFactsIngestResult:
    """Summary of an SEC companyfacts raw-ingest run."""

    universe: str
    tickers: tuple[str, ...]
    dry_run: bool
    run_id: int | None
    policy_name: str
    policy_version: int
    ticker_results: tuple[SecCompanyFactsTickerResult, ...] = ()

    @property
    def raw_responses_captured(self) -> int:
        return len(self.ticker_results)

    @property
    def bytes_captured(self) -> int:
        return sum(result.bytes_captured for result in self.ticker_results)


def ingest_sec_companyfacts(
    *,
    connection: Any,
    client: SECClient | None,
    universe: str,
    code_git_sha: str,
    tickers: Sequence[str] | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    sleep_seconds: float = 0.2,
    sleep: Callable[[float], Any] = time.sleep,
) -> SecCompanyFactsIngestResult:
    """Raw-ingest SEC companyfacts for persisted universe members."""
    normalized_universe = _required_label(universe, "universe")
    normalized_tickers = _ticker_filter(tickers)
    normalized_limit = _optional_limit(limit)
    normalized_sleep_seconds = _non_negative_number(sleep_seconds, "sleep_seconds")

    universe_repository = UniverseMembershipRepository(connection)
    all_members = universe_repository.list_members(normalized_universe)
    if not all_members:
        raise SecCompanyFactsIngestError(
            f"universe {normalized_universe} has no persisted members"
        )

    selected_members = _filter_members(all_members, normalized_tickers)
    ciks_by_security_id = _load_security_ciks(
        connection,
        tuple(member.security_id for member in selected_members),
    )
    ingest_members = _ingest_members(selected_members, ciks_by_security_id)
    if normalized_limit is not None:
        ingest_members = ingest_members[:normalized_limit]
    if not ingest_members:
        raise SecCompanyFactsIngestError(
            f"universe {normalized_universe} has no members matching selection"
        )

    policy = load_sec_companyfacts_policy(connection)
    if dry_run:
        return SecCompanyFactsIngestResult(
            universe=normalized_universe,
            tickers=tuple(member.ticker for member in ingest_members),
            dry_run=True,
            run_id=None,
            policy_name=policy.name,
            policy_version=policy.version,
        )
    if client is None:
        raise SecCompanyFactsIngestError("client is required unless dry_run is used")

    analytics_repository = AnalyticsRunRepository(connection)
    run_id: int | None = None

    try:
        run = analytics_repository.create_run(
            run_kind=SEC_COMPANYFACTS_RUN_KIND,
            code_git_sha=code_git_sha,
            available_at_policy_versions={policy.name: policy.version},
            parameters={
                "source": "sec",
                "universe": normalized_universe,
                "tickers": tuple(member.ticker for member in ingest_members),
                "ciks": {
                    member.ticker: member.cik for member in ingest_members
                },
                "endpoint_template": SEC_COMPANYFACTS_ENDPOINT_TEMPLATE,
                "audit_contract": SEC_COMPANYFACTS_AUDIT_CONTRACT,
                "limit": normalized_limit,
            },
            input_fingerprints={
                "universe_membership": [
                    _member_fingerprint(member) for member in selected_members
                ],
                "sec_ciks": [
                    {
                        "security_id": member.security_id,
                        "ticker": member.ticker,
                        "cik": member.cik,
                    }
                    for member in ingest_members
                ],
            },
        )
        run_id = run.id
        _commit(connection)

        ticker_results: list[SecCompanyFactsTickerResult] = []
        total = len(ingest_members)
        for index, member in enumerate(ingest_members, start=1):
            try:
                raw_response = client.fetch_companyfacts(member.cik)
            except SECHTTPError:
                _commit(connection)
                raise
            _commit(connection)
            ticker_results.append(
                SecCompanyFactsTickerResult(
                    ticker=member.ticker,
                    cik=member.cik,
                    raw_object_id=raw_response.raw_vault_result.raw_object_id,
                    raw_inserted=raw_response.raw_vault_result.inserted,
                    http_status=raw_response.http_status,
                    bytes_captured=len(raw_response.body),
                )
            )
            if normalized_sleep_seconds and index < total:
                sleep(normalized_sleep_seconds)

        analytics_repository.finish_run(run_id, status="succeeded")
        _commit(connection)
    except Exception:
        _rollback(connection)
        if run_id is not None:
            try:
                analytics_repository.finish_run(run_id, status="failed")
                _commit(connection)
            except Exception:
                _rollback(connection)
        raise

    return SecCompanyFactsIngestResult(
        universe=normalized_universe,
        tickers=tuple(member.ticker for member in ingest_members),
        dry_run=False,
        run_id=run_id,
        policy_name=policy.name,
        policy_version=policy.version,
        ticker_results=tuple(ticker_results),
    )


def load_sec_companyfacts_policy(
    connection: Any,
    *,
    version: int = SEC_COMPANYFACTS_POLICY_VERSION,
) -> SecCompanyFactsPolicy:
    """Load the configured companyfacts available-at policy by version."""
    normalized_version = _positive_int(version, "version")
    with _cursor(connection) as cursor:
        cursor.execute(
            _SELECT_POLICY_BY_NAME_VERSION_SQL,
            {
                "name": SEC_COMPANYFACTS_POLICY_NAME,
                "version": normalized_version,
            },
        )
        row = cursor.fetchone()
    if row is None:
        raise SecCompanyFactsIngestError(
            f"{SEC_COMPANYFACTS_POLICY_NAME} policy version "
            f"{normalized_version} was not found"
        )
    return _policy_record(row)


def _filter_members(
    members: Sequence[UniverseMember],
    tickers: frozenset[str] | None,
) -> tuple[UniverseMember, ...]:
    unique_members_by_security_id = {member.security_id: member for member in members}
    selected = tuple(
        sorted(
            unique_members_by_security_id.values(),
            key=lambda member: (member.ticker, member.security_id),
        )
    )
    if tickers is None:
        return selected

    selected_by_ticker = tuple(member for member in selected if member.ticker in tickers)
    found = {member.ticker for member in selected_by_ticker}
    missing = tickers - found
    if missing:
        raise SecCompanyFactsIngestError(
            "selected ticker(s) are not in universe "
            f"{', '.join(sorted(missing))}"
        )
    return selected_by_ticker


def _load_security_ciks(
    connection: Any,
    security_ids: Sequence[int],
) -> dict[int, str]:
    if not security_ids:
        return {}
    with _cursor(connection) as cursor:
        cursor.execute(
            _SELECT_SECURITY_CIKS_SQL,
            {"security_ids": list(security_ids)},
        )
        rows = cursor.fetchall()

    ciks_by_security_id: dict[int, str] = {}
    for row in rows:
        security_id = _row_int(row, "security_id", 0, "securities.id")
        cik = _row_str(row, "cik", 2, "securities.cik")
        if CIK_RE.fullmatch(cik) is None:
            ticker = _row_str(row, "ticker", 1, "securities.ticker")
            raise SecCompanyFactsIngestError(
                f"security {ticker} has invalid SEC CIK {cik!r}; expected 10 digits"
            )
        ciks_by_security_id[security_id] = cik
    return ciks_by_security_id


def _ingest_members(
    members: Sequence[UniverseMember],
    ciks_by_security_id: Mapping[int, str],
) -> tuple[SecCompanyFactsMember, ...]:
    ingest_members: list[SecCompanyFactsMember] = []
    for member in members:
        cik = ciks_by_security_id.get(member.security_id)
        if cik is None:
            raise SecCompanyFactsIngestError(
                f"security {member.ticker} is missing a SEC CIK"
            )
        ingest_members.append(
            SecCompanyFactsMember(
                security_id=member.security_id,
                ticker=member.ticker,
                cik=cik,
                universe_name=member.universe_name,
                valid_from=member.valid_from,
                valid_to=member.valid_to,
            )
        )
    return tuple(ingest_members)


def _member_fingerprint(member: UniverseMember) -> dict[str, Any]:
    return {
        "security_id": member.security_id,
        "ticker": member.ticker,
        "universe_name": member.universe_name,
        "valid_from": member.valid_from.isoformat(),
        "valid_to": None if member.valid_to is None else member.valid_to.isoformat(),
    }


def _policy_record(row: object) -> SecCompanyFactsPolicy:
    rule = _row_value(row, "rule", 3)
    if not isinstance(rule, Mapping):
        raise SecCompanyFactsIngestError(
            "available_at policy rule returned by database must be a mapping"
        )
    policy = SecCompanyFactsPolicy(
        id=_row_int(row, "id", 0, "available_at_policies.id"),
        name=_row_str(row, "name", 1, "available_at_policies.name"),
        version=_row_int(row, "version", 2, "available_at_policies.version"),
        rule=rule,
    )
    if policy.name != SEC_COMPANYFACTS_POLICY_NAME:
        raise SecCompanyFactsIngestError(
            f"available_at policy must be {SEC_COMPANYFACTS_POLICY_NAME}; "
            f"got {policy.name}"
        )
    return policy


def _required_label(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SecCompanyFactsIngestError(f"{name} must be a non-empty string")
    return value.strip()


def _ticker_filter(value: Sequence[str] | None) -> frozenset[str] | None:
    if value is None:
        return None
    tickers = frozenset(_ticker(ticker) for ticker in value)
    if not tickers:
        raise SecCompanyFactsIngestError("tickers must not be empty when provided")
    return tickers


def _ticker(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SecCompanyFactsIngestError("ticker must be a non-empty string")
    return value.strip().upper()


def _optional_limit(value: int | None) -> int | None:
    if value is None:
        return None
    return _positive_int(value, "limit")


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SecCompanyFactsIngestError(f"{name} must be a positive integer")
    return value


def _non_negative_number(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SecCompanyFactsIngestError(f"{name} must be non-negative")
    normalized = float(value)
    if normalized < 0:
        raise SecCompanyFactsIngestError(f"{name} must be non-negative")
    return normalized


def _row_value(row: object, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return row[index]  # type: ignore[index]


def _row_int(row: object, key: str, index: int, name: str) -> int:
    value = _row_value(row, key, index)
    if isinstance(value, bool) or not isinstance(value, int):
        raise SecCompanyFactsIngestError(
            f"{name} returned by database must be an integer"
        )
    return value


def _row_str(row: object, key: str, index: int, name: str) -> str:
    value = _row_value(row, key, index)
    if not isinstance(value, str) or not value.strip():
        raise SecCompanyFactsIngestError(
            f"{name} returned by database must be a non-empty string"
        )
    return value.strip()


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


def _commit(connection: Any) -> None:
    commit = getattr(connection, "commit", None)
    if commit is not None:
        commit()


def _rollback(connection: Any) -> None:
    rollback = getattr(connection, "rollback", None)
    if rollback is not None:
        rollback()


_SELECT_POLICY_BY_NAME_VERSION_SQL = """
SELECT id, name, version, rule
FROM silver.available_at_policies
WHERE name = %(name)s
  AND version = %(version)s
LIMIT 1;
""".strip()

_SELECT_SECURITY_CIKS_SQL = """
SELECT id AS security_id, ticker, cik
FROM silver.securities
WHERE id = ANY(%(security_ids)s::bigint[])
ORDER BY ticker;
""".strip()
