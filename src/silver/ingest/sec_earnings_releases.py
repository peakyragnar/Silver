"""Orchestrate SEC 8-K Item 2.02 earnings release ingest."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any

from silver.analytics import AnalyticsRunRepository
from silver.reference import UniverseMember, UniverseMembershipRepository
from silver.releases import (
    EarningsReleaseCandidate,
    EarningsReleaseEventRecord,
    EarningsReleaseEventRepository,
    EarningsReleaseParseError,
    EarningsReleasePolicy,
    json_payload,
    parse_earnings_release_exhibit,
    parse_sec_archive_index_documents,
    parse_sec_earnings_release_candidates,
    release_available_at,
    release_market_timing,
    select_earnings_exhibit,
)
from silver.sources.sec import SECClient, SECHTTPError
from silver.time.trading_calendar import TradingCalendar


SEC_EARNINGS_RELEASE_RUN_KIND = "sec_earnings_release_ingest"
SEC_EARNINGS_RELEASE_AUDIT_CONTRACT = "sec-earnings-release-v0"
DEFAULT_SINCE_DATE = date(2014, 1, 1)
DEFAULT_CANDIDATE_LIMIT = 1


class SecEarningsReleaseIngestError(RuntimeError):
    """Raised when SEC earnings release ingest cannot complete safely."""


@dataclass(frozen=True, slots=True)
class SecEarningsReleaseMember:
    """One universe security with SEC CIK."""

    security_id: int
    ticker: str
    cik: str
    universe_name: str
    valid_from: date
    valid_to: date | None


@dataclass(frozen=True, slots=True)
class SecEarningsReleaseEventResult:
    """Per-release ingest summary."""

    ticker: str
    accession_number: str
    fiscal_year: int
    fiscal_period: str
    period_end_date: date
    accepted_at: Any
    release_timing: str
    release_available_at: Any
    exhibit_document: str
    matched_confidence: str
    linked_income_fundamental_rows: int


@dataclass(frozen=True, slots=True)
class SecEarningsReleaseTickerResult:
    """Per-ticker release ingest summary."""

    ticker: str
    cik: str
    submissions_raw_object_id: int | None
    candidates_seen: int
    events_written: tuple[SecEarningsReleaseEventResult, ...]


@dataclass(frozen=True, slots=True)
class SecEarningsReleaseIngestResult:
    """Summary of SEC earnings release ingest."""

    universe: str
    tickers: tuple[str, ...]
    since_date: date
    candidate_limit: int
    dry_run: bool
    run_id: int | None
    policy_versions: Mapping[str, int]
    ticker_results: tuple[SecEarningsReleaseTickerResult, ...] = ()

    @property
    def events_written(self) -> int:
        return sum(len(result.events_written) for result in self.ticker_results)

    @property
    def linked_income_fundamental_rows(self) -> int:
        return sum(
            event.linked_income_fundamental_rows
            for result in self.ticker_results
            for event in result.events_written
        )


def ingest_sec_earnings_releases(
    *,
    connection: Any,
    client: SECClient | None,
    universe: str,
    code_git_sha: str,
    tickers: Sequence[str] | None = None,
    limit: int | None = None,
    since_date: date = DEFAULT_SINCE_DATE,
    candidate_limit: int = DEFAULT_CANDIDATE_LIMIT,
    dry_run: bool = False,
    sleep_seconds: float = 0.2,
    sleep: Callable[[float], Any] = time.sleep,
) -> SecEarningsReleaseIngestResult:
    """Ingest SEC Item 2.02 release events for persisted universe members."""
    normalized_universe = _required_label(universe, "universe")
    normalized_tickers = _ticker_filter(tickers)
    normalized_limit = _optional_limit(limit)
    normalized_since_date = _date(since_date, "since_date")
    normalized_candidate_limit = _positive_int(candidate_limit, "candidate_limit")
    normalized_sleep_seconds = _non_negative_number(sleep_seconds, "sleep_seconds")

    universe_repository = UniverseMembershipRepository(connection)
    all_members = universe_repository.list_members(normalized_universe)
    if not all_members:
        raise SecEarningsReleaseIngestError(
            f"universe {normalized_universe} has no persisted members"
        )

    selected_members = _filter_members(all_members, normalized_tickers)
    if normalized_limit is not None:
        selected_members = selected_members[:normalized_limit]
    ciks_by_security_id = _load_security_ciks(
        connection,
        tuple(member.security_id for member in selected_members),
    )
    ingest_members = _ingest_members(selected_members, ciks_by_security_id)
    if not ingest_members:
        raise SecEarningsReleaseIngestError(
            f"universe {normalized_universe} has no members matching selection"
        )

    repository = EarningsReleaseEventRepository(connection)
    policy = repository.load_policy()
    calendar = repository.load_trading_calendar()
    policy_versions = {policy.name: policy.version}
    if dry_run:
        return SecEarningsReleaseIngestResult(
            universe=normalized_universe,
            tickers=tuple(member.ticker for member in ingest_members),
            since_date=normalized_since_date,
            candidate_limit=normalized_candidate_limit,
            dry_run=True,
            run_id=None,
            policy_versions=policy_versions,
        )
    if client is None:
        raise SecEarningsReleaseIngestError("client is required unless dry_run is used")

    analytics_repository = AnalyticsRunRepository(connection)
    run_id: int | None = None

    try:
        run = analytics_repository.create_run(
            run_kind=SEC_EARNINGS_RELEASE_RUN_KIND,
            code_git_sha=code_git_sha,
            available_at_policy_versions=policy_versions,
            parameters={
                "source": "sec",
                "universe": normalized_universe,
                "tickers": tuple(member.ticker for member in ingest_members),
                "since_date": normalized_since_date.isoformat(),
                "candidate_limit": normalized_candidate_limit,
                "audit_contract": SEC_EARNINGS_RELEASE_AUDIT_CONTRACT,
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

        ticker_results: list[SecEarningsReleaseTickerResult] = []
        for member_index, member in enumerate(ingest_members, start=1):
            ticker_result = _ingest_member_releases(
                connection=connection,
                client=client,
                repository=repository,
                member=member,
                run_id=run_id,
                policy=policy,
                calendar=calendar,
                since_date=normalized_since_date,
                candidate_limit=normalized_candidate_limit,
            )
            ticker_results.append(ticker_result)
            if normalized_sleep_seconds and member_index < len(ingest_members):
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

    return SecEarningsReleaseIngestResult(
        universe=normalized_universe,
        tickers=tuple(member.ticker for member in ingest_members),
        since_date=normalized_since_date,
        candidate_limit=normalized_candidate_limit,
        dry_run=False,
        run_id=run_id,
        policy_versions=policy_versions,
        ticker_results=tuple(ticker_results),
    )


def _ingest_member_releases(
    *,
    connection: Any,
    client: SECClient,
    repository: EarningsReleaseEventRepository,
    member: SecEarningsReleaseMember,
    run_id: int,
    policy: EarningsReleasePolicy,
    calendar: TradingCalendar,
    since_date: date,
    candidate_limit: int,
) -> SecEarningsReleaseTickerResult:
    try:
        submissions_response = client.fetch_submissions(member.cik)
    except SECHTTPError:
        _commit(connection)
        raise
    _commit(connection)

    candidates = parse_sec_earnings_release_candidates(
        json_payload(submissions_response.body),
        since_date=since_date,
    )
    selected_candidates = candidates[:candidate_limit]
    event_results: list[SecEarningsReleaseEventResult] = []
    for candidate in selected_candidates:
        record = _candidate_record(
            connection=connection,
            client=client,
            member=member,
            run_id=run_id,
            policy=policy,
            calendar=calendar,
            submissions_raw_object_id=(
                submissions_response.raw_vault_result.raw_object_id
            ),
            candidate=candidate,
        )
        write_result = repository.write_events((record,))
        _commit(connection)
        linked_rows = repository.count_linked_income_fundamentals((record,))
        event_results.append(
            SecEarningsReleaseEventResult(
                ticker=member.ticker,
                accession_number=record.accession_number,
                fiscal_year=record.fiscal_year,
                fiscal_period=record.fiscal_period,
                period_end_date=record.period_end_date,
                accepted_at=record.accepted_at,
                release_timing=record.release_timing,
                release_available_at=record.release_available_at,
                exhibit_document=record.exhibit_document or "",
                matched_confidence=record.matched_confidence,
                linked_income_fundamental_rows=linked_rows,
            )
        )
        if write_result.rows_written != 1:
            raise SecEarningsReleaseIngestError("release event write failed")

    return SecEarningsReleaseTickerResult(
        ticker=member.ticker,
        cik=member.cik,
        submissions_raw_object_id=submissions_response.raw_vault_result.raw_object_id,
        candidates_seen=len(candidates),
        events_written=tuple(event_results),
    )


def _candidate_record(
    *,
    connection: Any,
    client: SECClient,
    member: SecEarningsReleaseMember,
    run_id: int,
    policy: EarningsReleasePolicy,
    calendar: TradingCalendar,
    submissions_raw_object_id: int,
    candidate: EarningsReleaseCandidate,
) -> EarningsReleaseEventRecord:
    try:
        archive_index_response = client.fetch_archive_index(
            cik=member.cik,
            accession_number=candidate.accession_number,
        )
    except SECHTTPError:
        _commit(connection)
        raise
    _commit(connection)
    documents = parse_sec_archive_index_documents(
        json_payload(archive_index_response.body)
    )
    exhibit = select_earnings_exhibit(
        documents,
        primary_document=candidate.primary_document,
    )
    if exhibit is None:
        raise SecEarningsReleaseIngestError(
            f"{member.ticker} {candidate.accession_number} had no EX-99.1 exhibit"
        )

    try:
        exhibit_response = client.fetch_archive_document(
            cik=member.cik,
            accession_number=candidate.accession_number,
            document_name=exhibit.name,
        )
    except SECHTTPError:
        _commit(connection)
        raise
    _commit(connection)

    try:
        evidence = parse_earnings_release_exhibit(exhibit_response.body)
    except EarningsReleaseParseError as exc:
        raise SecEarningsReleaseIngestError(str(exc)) from exc

    return EarningsReleaseEventRecord(
        security_id=member.security_id,
        accession_number=candidate.accession_number,
        form_type=candidate.form_type,
        item_codes=candidate.item_codes,
        filing_date=candidate.filing_date,
        report_date=candidate.report_date,
        accepted_at=candidate.accepted_at,
        release_timing=release_market_timing(
            candidate.accepted_at,
            calendar=calendar,
        ),
        release_available_at=release_available_at(
            candidate.accepted_at,
            policy=policy,
        ),
        available_at_policy_id=policy.id,
        fiscal_year=evidence.fiscal_year,
        fiscal_period=evidence.fiscal_period,
        period_end_date=evidence.period_end_date,
        primary_document=candidate.primary_document,
        exhibit_document=exhibit.name,
        submissions_raw_object_id=submissions_raw_object_id,
        archive_index_raw_object_id=archive_index_response.raw_vault_result.raw_object_id,
        exhibit_raw_object_id=exhibit_response.raw_vault_result.raw_object_id,
        normalized_by_run_id=run_id,
        matched_confidence=evidence.confidence,
        match_method=evidence.match_method,
        metadata={
            "audit_contract": SEC_EARNINGS_RELEASE_AUDIT_CONTRACT,
            "evidence_title": evidence.title,
            "evidence_excerpt": evidence.evidence_excerpt,
        },
    )


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
        raise SecEarningsReleaseIngestError(
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
        if not cik.isdigit() or len(cik) != 10:
            ticker = _row_str(row, "ticker", 1, "securities.ticker")
            raise SecEarningsReleaseIngestError(
                f"security {ticker} has invalid SEC CIK {cik!r}; expected 10 digits"
            )
        ciks_by_security_id[security_id] = cik
    return ciks_by_security_id


def _ingest_members(
    members: Sequence[UniverseMember],
    ciks_by_security_id: Mapping[int, str],
) -> tuple[SecEarningsReleaseMember, ...]:
    ingest_members: list[SecEarningsReleaseMember] = []
    for member in members:
        cik = ciks_by_security_id.get(member.security_id)
        if cik is None:
            raise SecEarningsReleaseIngestError(
                f"security {member.ticker} is missing a SEC CIK"
            )
        ingest_members.append(
            SecEarningsReleaseMember(
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


def _required_label(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SecEarningsReleaseIngestError(f"{name} must be a non-empty string")
    return value.strip()


def _ticker_filter(value: Sequence[str] | None) -> frozenset[str] | None:
    if value is None:
        return None
    tickers = frozenset(_ticker(ticker) for ticker in value)
    if not tickers:
        raise SecEarningsReleaseIngestError("tickers must not be empty when provided")
    return tickers


def _ticker(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SecEarningsReleaseIngestError("ticker must be a non-empty string")
    return value.strip().upper()


def _optional_limit(value: int | None) -> int | None:
    if value is None:
        return None
    return _positive_int(value, "limit")


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SecEarningsReleaseIngestError(f"{name} must be a positive integer")
    return value


def _non_negative_number(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SecEarningsReleaseIngestError(f"{name} must be non-negative")
    normalized = float(value)
    if normalized < 0:
        raise SecEarningsReleaseIngestError(f"{name} must be non-negative")
    return normalized


def _date(value: object, name: str) -> date:
    if isinstance(value, date):
        return value
    raise SecEarningsReleaseIngestError(f"{name} must be a date")


def _row_value(row: object, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return row[index]  # type: ignore[index]


def _row_int(row: object, key: str, index: int, name: str) -> int:
    value = _row_value(row, key, index)
    if isinstance(value, bool) or not isinstance(value, int):
        raise SecEarningsReleaseIngestError(f"{name} must be an integer")
    return value


def _row_str(row: object, key: str, index: int, name: str) -> str:
    value = _row_value(row, key, index)
    if not isinstance(value, str) or not value.strip():
        raise SecEarningsReleaseIngestError(f"{name} must be a non-empty string")
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


_SELECT_SECURITY_CIKS_SQL = """
SELECT id AS security_id, ticker, cik
FROM silver.securities
WHERE id = ANY(%(security_ids)s::bigint[])
ORDER BY ticker;
""".strip()
