"""Generate, validate, seed, and query the US equity trading calendar."""

from __future__ import annotations

import csv
import io
import shutil
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_PATH = ROOT / "config" / "trading_calendar.yaml"
DEFAULT_SEED_PATH = ROOT / "db" / "seed" / "trading_calendar.csv"
CANONICAL_HORIZONS = (5, 21, 63, 126, 252)
CSV_FIELDS = ("date", "is_session", "session_close", "is_early_close")
EXPECTED_SOURCE = "pandas_market_calendars"
EXPECTED_EARLY_CLOSE_RULE = "session_close_before_16_00_local"
REGULAR_CLOSE_LOCAL = time(hour=16)


class TradingCalendarValidationError(ValueError):
    """Raised when trading-calendar config or rows are malformed."""


class TradingCalendarGenerationError(RuntimeError):
    """Raised when the configured market calendar cannot be generated."""


class TradingCalendarSeedError(RuntimeError):
    """Raised when seed-file validation or database seeding fails."""


class MissingTradingCalendarRowsError(RuntimeError):
    """Raised when date advancement needs a calendar row that is absent."""


class NonTradingSessionError(ValueError):
    """Raised when a session-only helper receives a non-session as-of date."""


class InvalidTradingHorizonError(ValueError):
    """Raised when a caller uses a non-canonical trading-day horizon."""


@dataclass(frozen=True)
class TradingCalendarConfig:
    name: str
    source: str
    market: str
    timezone: str
    start_date: date
    end_date: date
    canonical_horizons: tuple[int, ...]
    early_close_rule: str


@dataclass(frozen=True)
class TradingCalendarRow:
    date: date
    is_session: bool
    session_close: datetime | None
    is_early_close: bool = False


class TradingCalendar:
    """In-memory helper for fail-closed trading-day arithmetic."""

    def __init__(self, rows: Sequence[TradingCalendarRow]) -> None:
        self._rows = tuple(sorted(rows, key=lambda row: row.date))
        self._rows_by_date: dict[date, TradingCalendarRow] = {}
        for row in self._rows:
            if row.date in self._rows_by_date:
                raise TradingCalendarValidationError(
                    f"duplicate trading-calendar row for {row.date.isoformat()}"
                )
            _validate_row(row)
            self._rows_by_date[row.date] = row

    @property
    def rows(self) -> tuple[TradingCalendarRow, ...]:
        return self._rows

    def row_for(self, day: date) -> TradingCalendarRow:
        row = self._rows_by_date.get(day)
        if row is None:
            raise MissingTradingCalendarRowsError(
                f"trading-calendar row is missing for {day.isoformat()}"
            )
        return row

    def advance_trading_days(self, asof_date: date, horizon_days: int) -> date:
        """Return the trading session exactly ``horizon_days`` after ``asof_date``."""
        if horizon_days not in CANONICAL_HORIZONS:
            allowed = ", ".join(str(horizon) for horizon in CANONICAL_HORIZONS)
            raise InvalidTradingHorizonError(
                f"horizon_days must be one of {allowed}; got {horizon_days}"
            )

        asof_row = self.row_for(asof_date)
        if not asof_row.is_session:
            raise NonTradingSessionError(
                f"asof_date must be a trading session; got {asof_date.isoformat()}"
            )

        sessions_seen = 0
        current = asof_date
        while sessions_seen < horizon_days:
            current += timedelta(days=1)
            row = self.row_for(current)
            if row.is_session:
                sessions_seen += 1
        return current

    def advance_canonical_horizons(
        self,
        asof_date: date,
        horizons: Sequence[int] = CANONICAL_HORIZONS,
    ) -> dict[int, date]:
        return {
            horizon: self.advance_trading_days(asof_date, horizon)
            for horizon in horizons
        }


def load_calendar_config(path: Path = DEFAULT_CONFIG_PATH) -> TradingCalendarConfig:
    """Read and validate the trading-calendar config."""
    if not path.exists():
        raise TradingCalendarValidationError(
            f"trading-calendar config does not exist: {path}"
        )

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise TradingCalendarValidationError(
            f"invalid YAML in trading-calendar config: {exc}"
        ) from exc

    return validate_calendar_config(raw)


def validate_calendar_config(raw: object) -> TradingCalendarConfig:
    if not isinstance(raw, Mapping):
        raise TradingCalendarValidationError("trading-calendar config must be a mapping")
    if raw.get("calendar_set_version") != 1:
        raise TradingCalendarValidationError("calendar_set_version must be 1")

    calendar = raw.get("calendar")
    if not isinstance(calendar, Mapping):
        raise TradingCalendarValidationError("calendar must be a mapping")

    name = _required_str(calendar, "name")
    source = _required_str(calendar, "source")
    if source != EXPECTED_SOURCE:
        raise TradingCalendarValidationError(
            f"calendar.source must be {EXPECTED_SOURCE}"
        )
    market = _required_str(calendar, "market")
    timezone_name = _required_str(calendar, "timezone")
    try:
        ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise TradingCalendarValidationError(
            f"calendar.timezone is unknown: {timezone_name}"
        ) from exc

    start_date = _parse_date(calendar.get("start_date"), "start_date")
    end_date = _parse_date(calendar.get("end_date"), "end_date")
    if end_date < start_date:
        raise TradingCalendarValidationError("calendar.end_date must be >= start_date")

    horizons_raw = calendar.get("canonical_horizons")
    if not isinstance(horizons_raw, list) or not all(
        isinstance(horizon, int) for horizon in horizons_raw
    ):
        raise TradingCalendarValidationError(
            "calendar.canonical_horizons must be a list of integers"
        )
    canonical_horizons = tuple(horizons_raw)
    if canonical_horizons != CANONICAL_HORIZONS:
        expected = ", ".join(str(horizon) for horizon in CANONICAL_HORIZONS)
        raise TradingCalendarValidationError(
            f"calendar.canonical_horizons must be [{expected}]"
        )

    early_close_rule = _required_str(calendar, "early_close_rule")
    if early_close_rule != EXPECTED_EARLY_CLOSE_RULE:
        raise TradingCalendarValidationError(
            f"calendar.early_close_rule must be {EXPECTED_EARLY_CLOSE_RULE}"
        )

    return TradingCalendarConfig(
        name=name,
        source=source,
        market=market,
        timezone=timezone_name,
        start_date=start_date,
        end_date=end_date,
        canonical_horizons=canonical_horizons,
        early_close_rule=early_close_rule,
    )


def generate_trading_calendar(
    config: TradingCalendarConfig,
) -> list[TradingCalendarRow]:
    """Generate one row per calendar date in the configured inclusive range."""
    try:
        import pandas_market_calendars as mcal
    except ImportError as exc:
        raise TradingCalendarGenerationError(
            "pandas-market-calendars is required to generate the trading calendar"
        ) from exc

    try:
        market_calendar = mcal.get_calendar(config.market)
        schedule = market_calendar.schedule(
            start_date=config.start_date.isoformat(),
            end_date=config.end_date.isoformat(),
        )
        early_closes = market_calendar.early_closes(schedule)
    except Exception as exc:  # pragma: no cover - library-specific error surface
        raise TradingCalendarGenerationError(
            f"could not generate {config.market} calendar: {exc}"
        ) from exc

    local_timezone = ZoneInfo(config.timezone)
    session_closes: dict[date, datetime] = {}
    for session_date, session in schedule.iterrows():
        close = session["market_close"].to_pydatetime().astimezone(timezone.utc)
        session_closes[session_date.date()] = close

    early_close_dates = {session_date.date() for session_date in early_closes.index}
    rows: list[TradingCalendarRow] = []
    for current in _date_range(config.start_date, config.end_date):
        session_close = session_closes.get(current)
        if session_close is None:
            rows.append(
                TradingCalendarRow(
                    date=current,
                    is_session=False,
                    session_close=None,
                    is_early_close=False,
                )
            )
            continue

        close_local = session_close.astimezone(local_timezone)
        rows.append(
            TradingCalendarRow(
                date=current,
                is_session=True,
                session_close=session_close,
                is_early_close=(
                    current in early_close_dates
                    or close_local.timetz().replace(tzinfo=None) < REGULAR_CLOSE_LOCAL
                ),
            )
        )

    validate_complete_calendar(rows, config.start_date, config.end_date)
    return rows


def validate_complete_calendar(
    rows: Sequence[TradingCalendarRow],
    start_date: date,
    end_date: date,
) -> None:
    """Validate one structurally sound row for every date in an inclusive range."""
    rows_by_date: dict[date, TradingCalendarRow] = {}
    for row in rows:
        if row.date in rows_by_date:
            raise TradingCalendarValidationError(
                f"duplicate trading-calendar row for {row.date.isoformat()}"
            )
        _validate_row(row)
        rows_by_date[row.date] = row

    expected_dates = tuple(_date_range(start_date, end_date))
    actual_dates = tuple(sorted(rows_by_date))
    if actual_dates != expected_dates:
        missing = [day for day in expected_dates if day not in rows_by_date]
        extra = [day for day in actual_dates if day < start_date or day > end_date]
        details: list[str] = []
        if missing:
            details.append(_summarize_dates("missing", missing))
        if extra:
            details.append(_summarize_dates("extra", extra))
        detail = "; ".join(details) if details else "date range is not contiguous"
        raise TradingCalendarValidationError(f"incomplete trading calendar: {detail}")


def load_seed_csv(path: Path = DEFAULT_SEED_PATH) -> list[TradingCalendarRow]:
    if not path.exists():
        raise TradingCalendarSeedError(f"trading-calendar seed CSV does not exist: {path}")

    rows: list[TradingCalendarRow] = []
    with path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        if tuple(reader.fieldnames or ()) != CSV_FIELDS:
            raise TradingCalendarSeedError(
                f"trading-calendar CSV columns must be {CSV_FIELDS}"
            )
        for line_number, raw_row in enumerate(reader, start=2):
            rows.append(_csv_row_to_calendar_row(raw_row, line_number))
    return rows


def rows_to_csv(rows: Sequence[TradingCalendarRow]) -> str:
    """Return deterministic CSV text for trading-calendar seed rows."""
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    for row in sorted(rows, key=lambda item: item.date):
        writer.writerow(
            {
                "date": row.date.isoformat(),
                "is_session": _bool_to_csv(row.is_session),
                "session_close": _datetime_to_csv(row.session_close),
                "is_early_close": _bool_to_csv(row.is_early_close),
            }
        )
    return output.getvalue()


def write_seed_csv(
    rows: Sequence[TradingCalendarRow],
    path: Path = DEFAULT_SEED_PATH,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(rows_to_csv(rows), encoding="utf-8")


def assert_seed_csv_matches(
    rows: Sequence[TradingCalendarRow],
    path: Path = DEFAULT_SEED_PATH,
) -> None:
    expected = rows_to_csv(rows)
    try:
        actual = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise TradingCalendarSeedError(
            f"trading-calendar seed CSV does not exist: {path}"
        ) from exc

    if actual != expected:
        raise TradingCalendarSeedError(
            "trading-calendar seed CSV is stale; run "
            "`python scripts/seed_trading_calendar.py --write-seed`"
        )


def build_upsert_sql(rows: Sequence[TradingCalendarRow]) -> str:
    if not rows:
        raise TradingCalendarValidationError("at least one trading-calendar row is required")

    values = [
        _row_values_sql(row) for row in sorted(rows, key=lambda item: item.date)
    ]
    values_sql = ",\n    ".join(values)
    return f"""
INSERT INTO silver.trading_calendar
    (date, is_session, session_close, is_early_close)
VALUES
    {values_sql}
ON CONFLICT (date) DO UPDATE SET
    is_session = EXCLUDED.is_session,
    session_close = EXCLUDED.session_close,
    is_early_close = EXCLUDED.is_early_close
WHERE
    silver.trading_calendar.is_session IS DISTINCT FROM EXCLUDED.is_session
    OR silver.trading_calendar.session_close IS DISTINCT FROM EXCLUDED.session_close
    OR silver.trading_calendar.is_early_close IS DISTINCT FROM EXCLUDED.is_early_close;
""".strip()


def seed_trading_calendar(
    rows: Sequence[TradingCalendarRow],
    database_url: str,
    *,
    psql_path: str | None = None,
) -> None:
    psql = psql_path or shutil.which("psql")
    if psql is None:
        raise TradingCalendarSeedError("psql is required to seed trading_calendar")

    result = subprocess.run(
        [psql, "-X", "-v", "ON_ERROR_STOP=1", "-q", "-d", database_url],
        input=build_upsert_sql(rows),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.replace(database_url, "[DATABASE_URL]").strip()
        detail = f": {stderr}" if stderr else ""
        raise TradingCalendarSeedError(
            f"psql failed with exit code {result.returncode}{detail}"
        )


def _validate_row(row: TradingCalendarRow) -> None:
    if row.is_session:
        if row.session_close is None:
            raise TradingCalendarValidationError(
                f"session row {row.date.isoformat()} must have session_close"
            )
        if row.session_close.tzinfo is None:
            raise TradingCalendarValidationError(
                f"session_close for {row.date.isoformat()} must include timezone"
            )
        return

    if row.session_close is not None:
        raise TradingCalendarValidationError(
            f"non-session row {row.date.isoformat()} must not have session_close"
        )
    if row.is_early_close:
        raise TradingCalendarValidationError(
            f"non-session row {row.date.isoformat()} cannot be an early close"
        )


def _required_str(raw: Mapping[str, object], field: str) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value:
        raise TradingCalendarValidationError(f"calendar.{field} must be a non-empty string")
    return value


def _parse_date(value: object, field: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        raise TradingCalendarValidationError(f"calendar.{field} must be a date string")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise TradingCalendarValidationError(
            f"calendar.{field} must use YYYY-MM-DD format"
        ) from exc


def _date_range(start_date: date, end_date: date) -> list[date]:
    days = (end_date - start_date).days
    return [start_date + timedelta(days=offset) for offset in range(days + 1)]


def _summarize_dates(label: str, dates: Sequence[date]) -> str:
    sample = ", ".join(day.isoformat() for day in dates[:5])
    suffix = "" if len(dates) <= 5 else f", ... ({len(dates)} total)"
    return f"{label}: {sample}{suffix}"


def _csv_row_to_calendar_row(
    raw_row: Mapping[str, str],
    line_number: int,
) -> TradingCalendarRow:
    try:
        row_date = date.fromisoformat(raw_row["date"])
        is_session = _csv_to_bool(raw_row["is_session"], "is_session", line_number)
        session_close = _csv_to_datetime(raw_row["session_close"], line_number)
        is_early_close = _csv_to_bool(
            raw_row["is_early_close"],
            "is_early_close",
            line_number,
        )
    except ValueError as exc:
        raise TradingCalendarSeedError(
            f"invalid trading-calendar CSV row at line {line_number}: {exc}"
        ) from exc

    row = TradingCalendarRow(
        date=row_date,
        is_session=is_session,
        session_close=session_close,
        is_early_close=is_early_close,
    )
    try:
        _validate_row(row)
    except TradingCalendarValidationError as exc:
        raise TradingCalendarSeedError(
            f"invalid trading-calendar CSV row at line {line_number}: {exc}"
        ) from exc
    return row


def _csv_to_bool(value: str, field: str, line_number: int) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise ValueError(f"{field} must be true or false at line {line_number}")


def _csv_to_datetime(value: str, line_number: int) -> datetime | None:
    if value == "":
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError(f"session_close must include timezone at line {line_number}")
    return parsed.astimezone(timezone.utc)


def _bool_to_csv(value: bool) -> str:
    return "true" if value else "false"


def _datetime_to_csv(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(timezone.utc).isoformat()


def _row_values_sql(row: TradingCalendarRow) -> str:
    return (
        "("
        f"{_sql_literal(row.date.isoformat())}::date, "
        f"{_sql_bool(row.is_session)}, "
        f"{_nullable_timestamptz(row.session_close)}, "
        f"{_sql_bool(row.is_early_close)}"
        ")"
    )


def _nullable_timestamptz(value: datetime | None) -> str:
    if value is None:
        return "NULL"
    return f"{_sql_literal(_datetime_to_csv(value))}::timestamptz"


def _sql_bool(value: bool) -> str:
    return "true" if value else "false"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
