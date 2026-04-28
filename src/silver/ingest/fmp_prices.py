"""Orchestrate FMP daily-price ingest through raw and normalized stores."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from silver.analytics import AnalyticsRunRepository
from silver.prices import DEFAULT_NORMALIZATION_VERSION, DailyPriceRepository
from silver.prices.daily import DailyPriceRow
from silver.reference import UniverseMember, UniverseMembershipRepository
from silver.sources.fmp import FMPClient, parse_historical_daily_prices


PRICE_NORMALIZATION_RUN_KIND = "price_normalization"


class FmpPriceIngestError(RuntimeError):
    """Raised when FMP price ingest cannot complete safely."""


@dataclass(frozen=True, slots=True)
class TickerIngestResult:
    """Per-security ingest summary."""

    ticker: str
    raw_object_id: int
    raw_inserted: bool
    rows_parsed: int
    rows_written: int


@dataclass(frozen=True, slots=True)
class FmpPriceIngestResult:
    """Summary of an FMP daily-price ingest run."""

    universe: str
    start_date: date
    end_date: date
    tickers: tuple[str, ...]
    dry_run: bool
    run_id: int | None
    ticker_results: tuple[TickerIngestResult, ...] = ()

    @property
    def raw_responses_captured(self) -> int:
        return len(self.ticker_results)

    @property
    def rows_parsed(self) -> int:
        return sum(result.rows_parsed for result in self.ticker_results)

    @property
    def rows_written(self) -> int:
        return sum(result.rows_written for result in self.ticker_results)


def ingest_fmp_prices(
    *,
    connection: Any,
    client: FMPClient | None,
    universe: str,
    start_date: date | None,
    end_date: date | None,
    code_git_sha: str,
    dry_run: bool = False,
    today: date | None = None,
) -> FmpPriceIngestResult:
    """Ingest FMP historical daily prices for persisted universe members."""
    normalized_universe = _required_label(universe, "universe")
    resolved_today = today or datetime.now(timezone.utc).date()
    _validate_date(resolved_today, "today")
    _validate_date_or_none(start_date, "start_date")
    _validate_date_or_none(end_date, "end_date")

    universe_repository = UniverseMembershipRepository(connection)
    all_members = universe_repository.list_members(normalized_universe)
    if not all_members:
        raise FmpPriceIngestError(
            f"universe {normalized_universe} has no persisted members"
        )

    resolved_start = start_date or min(member.valid_from for member in all_members)
    resolved_end = end_date or resolved_today
    _validate_date_range(resolved_start, resolved_end)

    members = _members_overlapping(all_members, resolved_start, resolved_end)
    if not members:
        raise FmpPriceIngestError(
            f"universe {normalized_universe} has no members overlapping "
            f"{resolved_start.isoformat()} to {resolved_end.isoformat()}"
        )
    tickers = _unique_tickers(members)

    if dry_run:
        return FmpPriceIngestResult(
            universe=normalized_universe,
            start_date=resolved_start,
            end_date=resolved_end,
            tickers=tickers,
            dry_run=True,
            run_id=None,
        )
    if client is None:
        raise FmpPriceIngestError("client is required unless dry_run is used")

    price_repository = DailyPriceRepository(connection)
    policy = price_repository.load_daily_price_policy()
    analytics_repository = AnalyticsRunRepository(connection)
    run_id: int | None = None

    try:
        run = analytics_repository.create_run(
            run_kind=PRICE_NORMALIZATION_RUN_KIND,
            code_git_sha=code_git_sha,
            available_at_policy_versions={policy.name: policy.version},
            parameters={
                "source": "fmp",
                "universe": normalized_universe,
                "start_date": resolved_start.isoformat(),
                "end_date": resolved_end.isoformat(),
                "tickers": tickers,
                "normalization_version": DEFAULT_NORMALIZATION_VERSION,
            },
            input_fingerprints={
                "universe_membership": [
                    _member_fingerprint(member) for member in members
                ],
            },
        )
        run_id = run.id
        _commit(connection)

        ticker_results: list[TickerIngestResult] = []
        for ticker in tickers:
            raw_response = client.fetch_historical_daily_prices(
                ticker,
                start_date=resolved_start,
                end_date=resolved_end,
            )
            _commit(connection)

            parsed_rows = parse_historical_daily_prices(
                _json_payload(raw_response.body, ticker)
            )
            rows = _rows_in_range(parsed_rows, resolved_start, resolved_end)
            _require_ticker(rows, ticker)
            write_result = price_repository.write_daily_prices(
                rows,
                raw_object_id=raw_response.raw_vault_result.raw_object_id,
                source=raw_response.source,
                available_at_policy_id=policy.id,
                normalized_by_run_id=run_id,
            )
            _commit(connection)
            ticker_results.append(
                TickerIngestResult(
                    ticker=ticker,
                    raw_object_id=raw_response.raw_vault_result.raw_object_id,
                    raw_inserted=raw_response.raw_vault_result.inserted,
                    rows_parsed=len(rows),
                    rows_written=write_result.rows_written,
                )
            )

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

    return FmpPriceIngestResult(
        universe=normalized_universe,
        start_date=resolved_start,
        end_date=resolved_end,
        tickers=tickers,
        dry_run=False,
        run_id=run_id,
        ticker_results=tuple(ticker_results),
    )


def _members_overlapping(
    members: Sequence[UniverseMember],
    start_date: date,
    end_date: date,
) -> tuple[UniverseMember, ...]:
    return tuple(
        member
        for member in members
        if member.valid_from <= end_date
        and (member.valid_to is None or member.valid_to >= start_date)
    )


def _unique_tickers(members: Sequence[UniverseMember]) -> tuple[str, ...]:
    return tuple(sorted({member.ticker.upper() for member in members}))


def _member_fingerprint(member: UniverseMember) -> dict[str, Any]:
    return {
        "security_id": member.security_id,
        "ticker": member.ticker,
        "universe_name": member.universe_name,
        "valid_from": member.valid_from.isoformat(),
        "valid_to": None if member.valid_to is None else member.valid_to.isoformat(),
    }


def _json_payload(body: bytes, ticker: str) -> Mapping[str, Any]:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise FmpPriceIngestError(
            f"FMP historical daily price response for {ticker} was not valid JSON"
        ) from exc
    if not isinstance(payload, Mapping):
        raise FmpPriceIngestError(
            f"FMP historical daily price response for {ticker} must be a JSON object"
        )
    return payload


def _rows_in_range(
    rows: Sequence[DailyPriceRow],
    start_date: date,
    end_date: date,
) -> tuple[DailyPriceRow, ...]:
    return tuple(row for row in rows if start_date <= row.date <= end_date)


def _require_ticker(rows: Sequence[DailyPriceRow], expected_ticker: str) -> None:
    expected = expected_ticker.upper()
    tickers = {row.ticker.upper() for row in rows}
    if tickers and tickers != {expected}:
        raise FmpPriceIngestError(
            "FMP historical daily price response ticker mismatch; "
            f"expected {expected}, got {', '.join(sorted(tickers))}"
        )


def _required_label(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FmpPriceIngestError(f"{name} must be a non-empty string")
    return value.strip()


def _validate_date_or_none(value: object, name: str) -> None:
    if value is None:
        return
    _validate_date(value, name)


def _validate_date(value: object, name: str) -> None:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise FmpPriceIngestError(f"{name} must be a date")


def _validate_date_range(start_date: date, end_date: date) -> None:
    if start_date > end_date:
        raise FmpPriceIngestError("start_date must be on or before end_date")


def _commit(connection: Any) -> None:
    commit = getattr(connection, "commit", None)
    if commit is not None:
        commit()


def _rollback(connection: Any) -> None:
    rollback = getattr(connection, "rollback", None)
    if rollback is not None:
        rollback()
