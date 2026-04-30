"""Read persisted point-in-time universe membership."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


class UniverseMembershipError(ValueError):
    """Raised when universe membership inputs or database rows are invalid."""


@dataclass(frozen=True, slots=True)
class UniverseMember:
    """One persisted security membership interval for a universe."""

    security_id: int
    ticker: str
    universe_name: str
    valid_from: date
    valid_to: date | None


class UniverseMembershipRepository:
    """Read members from ``silver.universe_membership``."""

    def __init__(self, connection: Any):
        self._connection = connection

    def list_members(
        self,
        universe_name: str,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> tuple[UniverseMember, ...]:
        """Return membership rows, optionally constrained to overlapping dates."""
        normalized_universe = _required_label(universe_name, "universe_name")
        _validate_date_or_none(start_date, "start_date")
        _validate_date_or_none(end_date, "end_date")
        if start_date is not None and end_date is not None and start_date > end_date:
            raise UniverseMembershipError("start_date must be on or before end_date")

        with _cursor(self._connection) as cursor:
            cursor.execute(
                _SELECT_MEMBERS_SQL,
                {
                    "universe_name": normalized_universe,
                    "start_date": start_date,
                    "end_date": end_date,
                },
            )
            rows = cursor.fetchall()

        return tuple(_member(row) for row in rows)


def _member(row: object) -> UniverseMember:
    return UniverseMember(
        security_id=_row_int(row, "security_id", 0, "securities.id"),
        ticker=_row_str(row, "ticker", 1, "securities.ticker").upper(),
        universe_name=_row_str(
            row,
            "universe_name",
            2,
            "universe_membership.universe_name",
        ),
        valid_from=_row_date(row, "valid_from", 3, "universe_membership.valid_from"),
        valid_to=_row_optional_date(
            row,
            "valid_to",
            4,
            "universe_membership.valid_to",
        ),
    )


def _required_label(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise UniverseMembershipError(f"{name} must be a non-empty string")
    return value.strip()


def _validate_date_or_none(value: object, name: str) -> None:
    if value is None:
        return
    if isinstance(value, datetime) or not isinstance(value, date):
        raise UniverseMembershipError(f"{name} must be a date")


def _row_value(row: object, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return row[index]  # type: ignore[index]


def _row_int(row: object, key: str, index: int, name: str) -> int:
    value = _row_value(row, key, index)
    if isinstance(value, bool) or not isinstance(value, int):
        raise UniverseMembershipError(f"{name} returned by database must be an integer")
    return value


def _row_str(row: object, key: str, index: int, name: str) -> str:
    value = _row_value(row, key, index)
    if not isinstance(value, str) or not value.strip():
        raise UniverseMembershipError(
            f"{name} returned by database must be a non-empty string"
        )
    return value.strip()


def _row_date(row: object, key: str, index: int, name: str) -> date:
    value = _row_value(row, key, index)
    if isinstance(value, datetime) or not isinstance(value, date):
        raise UniverseMembershipError(f"{name} returned by database must be a date")
    return value


def _row_optional_date(row: object, key: str, index: int, name: str) -> date | None:
    value = _row_value(row, key, index)
    if value is None:
        return None
    if isinstance(value, datetime) or not isinstance(value, date):
        raise UniverseMembershipError(f"{name} returned by database must be a date")
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


_SELECT_MEMBERS_SQL = """
SELECT
    security.id AS security_id,
    security.ticker,
    membership.universe_name,
    membership.valid_from,
    membership.valid_to
FROM silver.universe_membership membership
JOIN silver.securities security
    ON security.id = membership.security_id
WHERE membership.universe_name = %(universe_name)s
  AND (%(end_date)s::date IS NULL OR membership.valid_from <= %(end_date)s::date)
  AND (
      %(start_date)s::date IS NULL
      OR membership.valid_to IS NULL
      OR membership.valid_to >= %(start_date)s::date
  )
ORDER BY security.ticker, membership.valid_from;
""".strip()
