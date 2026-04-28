"""Load, validate, and seed Silver reference security data."""

from __future__ import annotations

import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Mapping, Sequence

import yaml


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_PATH = ROOT / "config" / "seed_reference_data.yaml"
FALSIFIER_UNIVERSE_NAME = "falsifier_seed"
REQUIRED_FALSIFIER_TICKERS = frozenset({"AAPL", "GOOGL", "JPM", "MSFT", "NVDA"})
TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.]{0,9}$")
CIK_RE = re.compile(r"^\d{10}$")
FISCAL_YEAR_END_RE = re.compile(r"^\d{2}-\d{2}$")


class SeedValidationError(ValueError):
    """Raised when a reference seed file is missing, duplicate, or malformed."""


class SeedApplyError(RuntimeError):
    """Raised when database seeding fails after validation succeeds."""


@dataclass(frozen=True)
class IdentifierSeed:
    identifier_type: str
    identifier: str
    valid_from: date
    valid_to: date | None = None


@dataclass(frozen=True)
class SecuritySeed:
    ticker: str
    name: str
    cik: str | None
    exchange: str | None
    asset_class: str = "equity"
    country: str = "US"
    currency: str = "USD"
    fiscal_year_end_md: str | None = None
    listed_at: date | None = None
    delisted_at: date | None = None
    identifiers: tuple[IdentifierSeed, ...] = ()


@dataclass(frozen=True)
class UniverseMembershipSeed:
    universe_name: str
    ticker: str
    valid_from: date
    valid_to: date | None = None
    reason: str | None = None


@dataclass(frozen=True)
class ReferenceSeedConfig:
    securities: tuple[SecuritySeed, ...]
    universe_memberships: tuple[UniverseMembershipSeed, ...]

    @property
    def falsifier_tickers(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                membership.ticker
                for membership in self.universe_memberships
                if membership.universe_name == FALSIFIER_UNIVERSE_NAME
            )
        )


def load_seed_file(path: Path = DEFAULT_CONFIG_PATH) -> ReferenceSeedConfig:
    """Read and validate a Silver reference seed config file."""
    if not path.exists():
        raise SeedValidationError(f"reference seed config does not exist: {path}")

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise SeedValidationError(f"invalid YAML in reference seed config: {exc}") from exc

    return validate_seed_config(raw)


def validate_seed_config(raw: object) -> ReferenceSeedConfig:
    """Validate parsed reference seed config data."""
    if not isinstance(raw, Mapping):
        raise SeedValidationError("reference seed config must be a mapping")
    if raw.get("seed_set_version") != 1:
        raise SeedValidationError("seed_set_version must be 1")

    raw_securities = raw.get("securities")
    if not isinstance(raw_securities, list):
        raise SeedValidationError("securities must be a list")
    raw_memberships = raw.get("universe_memberships")
    if not isinstance(raw_memberships, list):
        raise SeedValidationError("universe_memberships must be a list")

    securities = tuple(
        sorted(
            (
                _validate_security(raw_security, index)
                for index, raw_security in enumerate(raw_securities, start=1)
            ),
            key=lambda security: security.ticker,
        )
    )
    memberships = tuple(
        sorted(
            (
                _validate_membership(raw_membership, index)
                for index, raw_membership in enumerate(raw_memberships, start=1)
            ),
            key=lambda item: (item.universe_name, item.ticker, item.valid_from),
        )
    )

    _validate_cross_references(securities, memberships)
    return ReferenceSeedConfig(securities=securities, universe_memberships=memberships)


def build_seed_sql(seed_config: ReferenceSeedConfig) -> str:
    """Build deterministic idempotent SQL for Silver reference seed data."""
    if not seed_config.securities:
        raise SeedValidationError("at least one security seed is required")
    if not seed_config.universe_memberships:
        raise SeedValidationError("at least one universe membership seed is required")

    return "\n\n".join(
        (
            _build_securities_upsert(seed_config.securities),
            _build_identifiers_upsert(seed_config.securities),
            _build_universe_memberships_upsert(seed_config.universe_memberships),
        )
    )


def seed_reference_data(
    seed_config: ReferenceSeedConfig,
    database_url: str,
    *,
    psql_path: str | None = None,
) -> None:
    """Insert/update securities and universe membership through psql."""
    psql = psql_path or shutil.which("psql")
    if psql is None:
        raise SeedApplyError("psql is required to seed reference data")

    result = subprocess.run(
        [psql, "-X", "-v", "ON_ERROR_STOP=1", "-q", "-d", database_url],
        input=build_seed_sql(seed_config),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.replace(database_url, "[DATABASE_URL]").strip()
        detail = f": {stderr}" if stderr else ""
        raise SeedApplyError(f"psql failed with exit code {result.returncode}{detail}")


def _validate_security(raw_security: object, index: int) -> SecuritySeed:
    if not isinstance(raw_security, Mapping):
        raise SeedValidationError(f"security #{index} must be a mapping")

    ticker = _required_str(raw_security, "ticker", f"security #{index}")
    if TICKER_RE.fullmatch(ticker) is None:
        raise SeedValidationError(f"security {ticker} ticker is malformed")

    name = _required_str(raw_security, "name", f"security {ticker}")
    cik = _optional_str(raw_security, "cik", f"security {ticker}")
    if cik is not None and CIK_RE.fullmatch(cik) is None:
        raise SeedValidationError(f"security {ticker} cik must be 10 digits")

    exchange = _optional_str(raw_security, "exchange", f"security {ticker}")
    asset_class = _optional_str(
        raw_security,
        "asset_class",
        f"security {ticker}",
        default="equity",
    )
    country = _optional_str(
        raw_security,
        "country",
        f"security {ticker}",
        default="US",
    )
    currency = _optional_str(
        raw_security,
        "currency",
        f"security {ticker}",
        default="USD",
    )
    fiscal_year_end_md = _optional_str(
        raw_security,
        "fiscal_year_end_md",
        f"security {ticker}",
    )
    if (
        fiscal_year_end_md is not None
        and FISCAL_YEAR_END_RE.fullmatch(fiscal_year_end_md) is None
    ):
        raise SeedValidationError(
            f"security {ticker} fiscal_year_end_md must use MM-DD format"
        )

    listed_at = _optional_date(raw_security, "listed_at", f"security {ticker}")
    delisted_at = _optional_date(raw_security, "delisted_at", f"security {ticker}")
    if listed_at is not None and delisted_at is not None and delisted_at < listed_at:
        raise SeedValidationError(
            f"security {ticker} delisted_at must be on or after listed_at"
        )

    raw_identifiers = raw_security.get("identifiers")
    if not isinstance(raw_identifiers, list) or not raw_identifiers:
        raise SeedValidationError(f"security {ticker} must have identifiers")
    identifiers = tuple(
        sorted(
            (
                _validate_identifier(raw_identifier, ticker, identifier_index)
                for identifier_index, raw_identifier in enumerate(
                    raw_identifiers,
                    start=1,
                )
            ),
            key=lambda item: (item.identifier_type, item.valid_from),
        )
    )
    _validate_security_identifier_coverage(ticker, cik, identifiers)

    return SecuritySeed(
        ticker=ticker,
        name=name,
        cik=cik,
        exchange=exchange,
        asset_class=asset_class,
        country=country,
        currency=currency,
        fiscal_year_end_md=fiscal_year_end_md,
        listed_at=listed_at,
        delisted_at=delisted_at,
        identifiers=identifiers,
    )


def _validate_identifier(
    raw_identifier: object,
    ticker: str,
    index: int,
) -> IdentifierSeed:
    if not isinstance(raw_identifier, Mapping):
        raise SeedValidationError(
            f"security {ticker} identifier #{index} must be a mapping"
        )

    identifier_type = _required_str(
        raw_identifier,
        "type",
        f"security {ticker} identifier #{index}",
    ).lower()
    identifier = _required_str(
        raw_identifier,
        "identifier",
        f"security {ticker} identifier #{index}",
    )
    valid_from = _required_date(
        raw_identifier,
        "valid_from",
        f"security {ticker} identifier {identifier_type}",
    )
    valid_to = _optional_date(
        raw_identifier,
        "valid_to",
        f"security {ticker} identifier {identifier_type}",
    )
    if valid_to is not None and valid_to < valid_from:
        raise SeedValidationError(
            f"security {ticker} identifier {identifier_type} "
            "valid_to must be on or after valid_from"
        )

    return IdentifierSeed(
        identifier_type=identifier_type,
        identifier=identifier,
        valid_from=valid_from,
        valid_to=valid_to,
    )


def _validate_security_identifier_coverage(
    ticker: str,
    cik: str | None,
    identifiers: Sequence[IdentifierSeed],
) -> None:
    keys = [(identifier.identifier_type, identifier.valid_from) for identifier in identifiers]
    if len(keys) != len(set(keys)):
        raise SeedValidationError(f"security {ticker} has duplicate identifier ranges")

    identifier_pairs = {
        (identifier.identifier_type, identifier.identifier) for identifier in identifiers
    }
    if ("ticker", ticker) not in identifier_pairs:
        raise SeedValidationError(f"security {ticker} must include ticker identifier")
    if cik is not None and ("cik", cik) not in identifier_pairs:
        raise SeedValidationError(f"security {ticker} must include cik identifier")


def _validate_membership(raw_membership: object, index: int) -> UniverseMembershipSeed:
    if not isinstance(raw_membership, Mapping):
        raise SeedValidationError(f"membership #{index} must be a mapping")

    context = f"membership #{index}"
    universe_name = _required_str(raw_membership, "universe_name", context)
    ticker = _required_str(raw_membership, "ticker", context)
    valid_from = _required_date(raw_membership, "valid_from", context)
    valid_to = _optional_date(raw_membership, "valid_to", context)
    if valid_to is not None and valid_to < valid_from:
        raise SeedValidationError(f"{context} valid_to must be on or after valid_from")
    reason = _optional_str(raw_membership, "reason", context)

    return UniverseMembershipSeed(
        universe_name=universe_name,
        ticker=ticker,
        valid_from=valid_from,
        valid_to=valid_to,
        reason=reason,
    )


def _validate_cross_references(
    securities: Sequence[SecuritySeed],
    memberships: Sequence[UniverseMembershipSeed],
) -> None:
    security_tickers = [security.ticker for security in securities]
    if len(security_tickers) != len(set(security_tickers)):
        raise SeedValidationError("security tickers must be unique")
    security_ticker_set = set(security_tickers)
    if security_ticker_set != REQUIRED_FALSIFIER_TICKERS:
        raise SeedValidationError(
            "seed securities must contain exactly: "
            f"{', '.join(sorted(REQUIRED_FALSIFIER_TICKERS))}; "
            f"found: {', '.join(sorted(security_ticker_set))}"
        )

    membership_keys = [
        (membership.universe_name, membership.ticker, membership.valid_from)
        for membership in memberships
    ]
    if len(membership_keys) != len(set(membership_keys)):
        raise SeedValidationError("universe membership rows must be unique")

    for membership in memberships:
        if membership.ticker not in security_ticker_set:
            raise SeedValidationError(
                f"membership references unknown security ticker {membership.ticker}"
            )
        if membership.universe_name != FALSIFIER_UNIVERSE_NAME:
            raise SeedValidationError(
                f"unsupported universe_name {membership.universe_name}; "
                f"expected {FALSIFIER_UNIVERSE_NAME}"
            )

    falsifier_tickers = {
        membership.ticker
        for membership in memberships
        if membership.universe_name == FALSIFIER_UNIVERSE_NAME
    }
    if falsifier_tickers != REQUIRED_FALSIFIER_TICKERS:
        raise SeedValidationError(
            f"{FALSIFIER_UNIVERSE_NAME} must contain exactly: "
            f"{', '.join(sorted(REQUIRED_FALSIFIER_TICKERS))}; "
            f"found: {', '.join(sorted(falsifier_tickers))}"
        )


def _build_securities_upsert(securities: Sequence[SecuritySeed]) -> str:
    values_sql = ",\n    ".join(
        _security_values_sql(security)
        for security in sorted(securities, key=lambda item: item.ticker)
    )
    changed_columns = (
        "name",
        "cik",
        "exchange",
        "asset_class",
        "country",
        "currency",
        "fiscal_year_end_md",
        "listed_at",
        "delisted_at",
    )
    change_predicate = "\n    OR ".join(
        f"silver.securities.{column} IS DISTINCT FROM EXCLUDED.{column}"
        for column in changed_columns
    )
    return f"""
INSERT INTO silver.securities
    (
        ticker,
        name,
        cik,
        exchange,
        asset_class,
        country,
        currency,
        fiscal_year_end_md,
        listed_at,
        delisted_at
    )
VALUES
    {values_sql}
ON CONFLICT (ticker) DO UPDATE SET
    name = EXCLUDED.name,
    cik = EXCLUDED.cik,
    exchange = EXCLUDED.exchange,
    asset_class = EXCLUDED.asset_class,
    country = EXCLUDED.country,
    currency = EXCLUDED.currency,
    fiscal_year_end_md = EXCLUDED.fiscal_year_end_md,
    listed_at = EXCLUDED.listed_at,
    delisted_at = EXCLUDED.delisted_at,
    updated_at = now()
WHERE
    {change_predicate};
""".strip()


def _build_identifiers_upsert(securities: Sequence[SecuritySeed]) -> str:
    values = []
    for security in sorted(securities, key=lambda item: item.ticker):
        for identifier in sorted(
            security.identifiers,
            key=lambda item: (item.identifier_type, item.identifier, item.valid_from),
        ):
            values.append(_identifier_values_sql(security.ticker, identifier))
    values_sql = ",\n    ".join(values)
    return f"""
WITH identifier_seed(ticker, identifier_type, identifier, valid_from, valid_to) AS (
    VALUES
    {values_sql}
)
INSERT INTO silver.security_identifiers
    (security_id, identifier_type, identifier, valid_from, valid_to)
SELECT
    security.id,
    seed.identifier_type,
    seed.identifier,
    seed.valid_from,
    seed.valid_to
FROM identifier_seed seed
JOIN silver.securities security
    ON security.ticker = seed.ticker
ON CONFLICT (security_id, identifier_type, valid_from) DO UPDATE SET
    identifier = EXCLUDED.identifier,
    valid_to = EXCLUDED.valid_to
WHERE
    silver.security_identifiers.identifier IS DISTINCT FROM EXCLUDED.identifier
    OR silver.security_identifiers.valid_to IS DISTINCT FROM EXCLUDED.valid_to;
""".strip()


def _build_universe_memberships_upsert(
    memberships: Sequence[UniverseMembershipSeed],
) -> str:
    values_sql = ",\n    ".join(
        _membership_values_sql(membership)
        for membership in sorted(
            memberships,
            key=lambda item: (item.universe_name, item.ticker, item.valid_from),
        )
    )
    return f"""
WITH membership_seed(ticker, universe_name, valid_from, valid_to, reason) AS (
    VALUES
    {values_sql}
)
INSERT INTO silver.universe_membership
    (security_id, universe_name, valid_from, valid_to, reason)
SELECT
    security.id,
    seed.universe_name,
    seed.valid_from,
    seed.valid_to,
    seed.reason
FROM membership_seed seed
JOIN silver.securities security
    ON security.ticker = seed.ticker
ON CONFLICT (security_id, universe_name, valid_from) DO UPDATE SET
    valid_to = EXCLUDED.valid_to,
    reason = EXCLUDED.reason
WHERE
    silver.universe_membership.valid_to IS DISTINCT FROM EXCLUDED.valid_to
    OR silver.universe_membership.reason IS DISTINCT FROM EXCLUDED.reason;
""".strip()


def _security_values_sql(security: SecuritySeed) -> str:
    return (
        "("
        f"{_sql_literal(security.ticker)}, "
        f"{_sql_literal(security.name)}, "
        f"{_nullable_text(security.cik)}, "
        f"{_nullable_text(security.exchange)}, "
        f"{_sql_literal(security.asset_class)}, "
        f"{_sql_literal(security.country)}, "
        f"{_sql_literal(security.currency)}, "
        f"{_nullable_text(security.fiscal_year_end_md)}, "
        f"{_nullable_date(security.listed_at)}, "
        f"{_nullable_date(security.delisted_at)}"
        ")"
    )


def _identifier_values_sql(ticker: str, identifier: IdentifierSeed) -> str:
    return (
        "("
        f"{_sql_literal(ticker)}, "
        f"{_sql_literal(identifier.identifier_type)}, "
        f"{_sql_literal(identifier.identifier)}, "
        f"{_date_literal(identifier.valid_from)}, "
        f"{_nullable_date(identifier.valid_to)}"
        ")"
    )


def _membership_values_sql(membership: UniverseMembershipSeed) -> str:
    return (
        "("
        f"{_sql_literal(membership.ticker)}, "
        f"{_sql_literal(membership.universe_name)}, "
        f"{_date_literal(membership.valid_from)}, "
        f"{_nullable_date(membership.valid_to)}, "
        f"{_nullable_text(membership.reason)}"
        ")"
    )


def _required_str(raw: Mapping[str, object], field: str, context: str) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value:
        raise SeedValidationError(f"{context} must have a non-empty {field}")
    return value


def _optional_str(
    raw: Mapping[str, object],
    field: str,
    context: str,
    *,
    default: str | None = None,
) -> str | None:
    value = raw.get(field, default)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise SeedValidationError(f"{context} {field} must be a non-empty string or null")
    return value


def _required_date(raw: Mapping[str, object], field: str, context: str) -> date:
    value = raw.get(field)
    if not isinstance(value, str) or not value:
        raise SeedValidationError(f"{context} must have a non-empty {field}")
    return _parse_date(value, field, context)


def _optional_date(
    raw: Mapping[str, object],
    field: str,
    context: str,
) -> date | None:
    value = raw.get(field)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise SeedValidationError(f"{context} {field} must be a date string or null")
    return _parse_date(value, field, context)


def _parse_date(value: str, field: str, context: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SeedValidationError(f"{context} {field} must be an ISO date") from exc


def _nullable_date(value: date | None) -> str:
    if value is None:
        return "NULL"
    return _date_literal(value)


def _date_literal(value: date) -> str:
    return f"{_sql_literal(value.isoformat())}::date"


def _nullable_text(value: str | None) -> str:
    if value is None:
        return "NULL"
    return _sql_literal(value)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
