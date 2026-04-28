#!/usr/bin/env python
"""Materialize Phase 1 forward-return labels from normalized prices."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from silver.labels.forward_returns import ForwardReturnLabelInputError  # noqa: E402
from silver.labels.materialize import build_forward_label_records  # noqa: E402
from silver.labels.repository import (  # noqa: E402
    ForwardLabelPersistenceError,
    ForwardLabelPriceObservation,
    ForwardLabelRepository,
)
from silver.prices.daily import DailyPriceRow  # noqa: E402
from silver.reference.seed_data import FALSIFIER_UNIVERSE_NAME  # noqa: E402
from silver.time.trading_calendar import (  # noqa: E402
    CANONICAL_HORIZONS,
    MissingTradingCalendarRowsError,
    TradingCalendar,
    TradingCalendarRow,
)


class MaterializeForwardLabelsError(RuntimeError):
    """Raised when the materialization command cannot complete."""


@dataclass(frozen=True, slots=True)
class MaterializationCliResult:
    run_id: int
    records_seen: int
    rows_changed: int
    skipped_count: int


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate materialization logic without connecting to Postgres",
    )
    parser.add_argument(
        "--universe",
        default=FALSIFIER_UNIVERSE_NAME,
        help=f"universe name to materialize; defaults to {FALSIFIER_UNIVERSE_NAME}",
    )
    parser.add_argument(
        "--start-date",
        type=_parse_date,
        help="inclusive label_date lower bound in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        help="inclusive label_date upper bound in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection URL; defaults to DATABASE_URL",
    )
    parser.add_argument(
        "--label-version",
        type=int,
        default=1,
        help="forward label version to materialize",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.start_date and args.end_date and args.start_date > args.end_date:
            raise MaterializeForwardLabelsError(
                "--start-date must be less than or equal to --end-date"
            )

        if args.check:
            _run_check()
            print(
                "OK: materialize_forward_labels check passed for "
                f"{len(CANONICAL_HORIZONS)} canonical horizon(s)"
            )
            return 0

        if not args.database_url:
            raise MaterializeForwardLabelsError(
                "DATABASE_URL is required unless --check is used"
            )

        connection = _connect(args.database_url)
        try:
            result, skipped_by_reason = materialize_forward_labels(
                connection=connection,
                universe_name=args.universe,
                start_date=args.start_date,
                end_date=args.end_date,
                label_version=args.label_version,
            )
            _commit(connection)
        except Exception:
            _rollback(connection)
            raise
        finally:
            _close(connection)

    except (
        ForwardLabelPersistenceError,
        ForwardReturnLabelInputError,
        MaterializeForwardLabelsError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        "OK: materialized "
        f"{result.records_seen} forward-label record(s); "
        f"{result.rows_changed} inserted/updated; "
        f"{result.skipped_count} skipped; run_id={result.run_id}"
    )
    for reason, count in sorted(skipped_by_reason.items()):
        print(f"skipped {reason}: {count}")
    return 0


def materialize_forward_labels(
    *,
    connection: object,
    universe_name: str,
    start_date: date | None,
    end_date: date | None,
    label_version: int,
) -> tuple[MaterializationCliResult, Counter[str]]:
    repository = ForwardLabelRepository(connection)
    calendar = repository.load_trading_calendar()
    price_end_date = _price_end_date(calendar, end_date)

    prices = repository.load_universe_price_observations(
        universe_name=universe_name,
        label_start_date=start_date,
        label_end_date=end_date,
        price_end_date=price_end_date,
    )
    label_dates_by_security = repository.load_label_dates_by_security(
        universe_name=universe_name,
        label_start_date=start_date,
        label_end_date=end_date,
    )
    policy_versions = repository.load_available_at_policy_versions(
        tuple({price.available_at_policy_id for price in prices})
    )
    run_id = repository.create_label_generation_run(
        code_git_sha=_code_git_sha(),
        available_at_policy_versions=policy_versions,
        parameters={
            "universe": universe_name,
            "start_date": start_date,
            "end_date": end_date,
            "horizons": CANONICAL_HORIZONS,
            "label_version": label_version,
        },
        input_fingerprints=_input_fingerprints(prices, label_dates_by_security),
    )

    try:
        materialized = build_forward_label_records(
            prices=prices,
            calendar=calendar,
            label_dates_by_security=label_dates_by_security,
            computed_by_run_id=run_id,
            horizons=CANONICAL_HORIZONS,
            label_version=label_version,
        )
        write_result = repository.write_forward_labels(materialized.records)
        repository.finish_label_generation_run(run_id, status="succeeded")
    except Exception:
        repository.finish_label_generation_run(run_id, status="failed")
        raise

    skipped_by_reason = Counter(skip.reason.value for skip in materialized.skipped)
    return (
        MaterializationCliResult(
            run_id=run_id,
            records_seen=write_result.records_seen,
            rows_changed=write_result.rows_changed,
            skipped_count=len(materialized.skipped),
        ),
        skipped_by_reason,
    )


def _price_end_date(calendar: TradingCalendar, end_date: date | None) -> date | None:
    if end_date is None:
        return None
    eligible_sessions = [
        row.date for row in calendar.rows if row.is_session and row.date <= end_date
    ]
    if not eligible_sessions:
        return end_date
    last_label_date = eligible_sessions[-1]
    try:
        return max(
            calendar.advance_trading_days(last_label_date, horizon)
            for horizon in CANONICAL_HORIZONS
        )
    except MissingTradingCalendarRowsError as exc:
        raise MaterializeForwardLabelsError(
            "trading calendar does not cover the requested end date plus "
            f"{max(CANONICAL_HORIZONS)} sessions"
        ) from exc


def _input_fingerprints(
    prices: Sequence[ForwardLabelPriceObservation],
    label_dates_by_security: dict[int, tuple[date, ...]],
) -> dict[str, object]:
    price_dates = [price.row.date for price in prices]
    label_date_count = sum(len(label_dates) for label_dates in label_dates_by_security.values())
    return {
        "price_row_count": len(prices),
        "price_date_min": min(price_dates) if price_dates else None,
        "price_date_max": max(price_dates) if price_dates else None,
        "security_count": len({price.security_id for price in prices}),
        "label_date_count": label_date_count,
    }


def _run_check() -> None:
    calendar = _check_calendar()
    sessions = [row.date for row in calendar.rows if row.is_session]
    prices = [
        _check_price_observation(
            session,
            index=index,
            policy_id=3,
        )
        for index, session in enumerate(sessions)
    ]
    result = build_forward_label_records(
        prices=prices,
        calendar=calendar,
        label_dates_by_security={101: (sessions[0],)},
        computed_by_run_id=1,
    )
    if len(result.records) != len(CANONICAL_HORIZONS) or result.skipped:
        raise MaterializeForwardLabelsError(
            "offline materialization check did not produce canonical labels"
        )
    for record in result.records:
        if record.available_at < record.horizon_close_at:
            raise MaterializeForwardLabelsError(
                "offline materialization check produced an early label"
            )


def _check_calendar() -> TradingCalendar:
    rows: list[TradingCalendarRow] = []
    current = date(2024, 1, 2)
    sessions_seen = 0
    while sessions_seen <= max(CANONICAL_HORIZONS):
        is_session = current.weekday() < 5
        rows.append(
            TradingCalendarRow(
                date=current,
                is_session=is_session,
                session_close=(
                    datetime.combine(current, datetime.min.time(), tzinfo=timezone.utc)
                    + timedelta(hours=21)
                    if is_session
                    else None
                ),
            )
        )
        if is_session:
            sessions_seen += 1
        current += timedelta(days=1)
    return TradingCalendar(rows)


def _check_price_observation(
    day: date,
    *,
    index: int,
    policy_id: int,
) -> ForwardLabelPriceObservation:
    value = Decimal("100") + Decimal(index)
    return ForwardLabelPriceObservation(
        security_id=101,
        row=DailyPriceRow(
            ticker="AAA",
            date=day,
            open=value,
            high=value,
            low=value,
            close=value,
            adj_close=value,
            volume=1000,
            source="check",
        ),
        available_at=(
            datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
            + timedelta(hours=23)
        ),
        available_at_policy_id=policy_id,
    )


def _connect(database_url: str) -> object:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise MaterializeForwardLabelsError(
            "psycopg is required to materialize labels against Postgres"
        ) from exc
    return psycopg.connect(database_url, row_factory=dict_row)


def _code_git_sha() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise MaterializeForwardLabelsError("could not determine code_git_sha")
    return result.stdout.strip()


def _commit(connection: object) -> None:
    commit = getattr(connection, "commit", None)
    if commit is not None:
        commit()


def _rollback(connection: object) -> None:
    rollback = getattr(connection, "rollback", None)
    if rollback is not None:
        rollback()


def _close(connection: object) -> None:
    close = getattr(connection, "close", None)
    if close is not None:
        close()


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD format") from exc


if __name__ == "__main__":
    raise SystemExit(main())
