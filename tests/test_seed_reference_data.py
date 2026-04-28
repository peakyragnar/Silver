from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from silver.reference.seed_data import (
    DEFAULT_CONFIG_PATH,
    FALSIFIER_UNIVERSE_NAME,
    REQUIRED_FALSIFIER_TICKERS,
    SeedValidationError,
    build_seed_sql,
    load_seed_file,
    validate_seed_config,
)


ROOT = Path(__file__).resolve().parents[1]
SEED_SCRIPT = ROOT / "scripts" / "seed_reference_data.py"


def test_default_seed_config_contains_exact_falsifier_universe() -> None:
    seed_config = load_seed_file(DEFAULT_CONFIG_PATH)

    assert seed_config.falsifier_tickers == tuple(sorted(REQUIRED_FALSIFIER_TICKERS))
    assert seed_config.falsifier_tickers == ("AAPL", "GOOGL", "JPM", "MSFT", "NVDA")


def test_default_seed_config_identifiers_cover_tickers_and_ciks() -> None:
    seed_config = load_seed_file(DEFAULT_CONFIG_PATH)

    for security in seed_config.securities:
        identifier_keys = {
            (identifier.identifier_type, identifier.identifier)
            for identifier in security.identifiers
        }

        assert ("ticker", security.ticker) in identifier_keys
        assert security.cik is not None
        assert ("cik", security.cik) in identifier_keys


def test_default_seed_config_has_valid_membership_dates() -> None:
    seed_config = load_seed_file(DEFAULT_CONFIG_PATH)

    for membership in seed_config.universe_memberships:
        assert membership.universe_name == FALSIFIER_UNIVERSE_NAME
        assert membership.valid_from.isoformat() == "2014-04-03"
        assert membership.valid_to is None


def test_default_seed_config_memberships_reference_known_securities_only() -> None:
    seed_config = load_seed_file(DEFAULT_CONFIG_PATH)
    security_tickers = {security.ticker for security in seed_config.securities}
    membership_tickers = {
        membership.ticker for membership in seed_config.universe_memberships
    }

    assert membership_tickers == security_tickers


def test_build_seed_sql_is_deterministic_idempotent_upsert() -> None:
    seed_config = load_seed_file(DEFAULT_CONFIG_PATH)

    sql = build_seed_sql(seed_config)

    assert sql == build_seed_sql(seed_config)
    assert "ON CONFLICT (ticker) DO UPDATE SET" in sql
    assert "ON CONFLICT (security_id, identifier_type, valid_from)" in sql
    assert "ON CONFLICT (security_id, universe_name, valid_from)" in sql
    assert "IS DISTINCT FROM EXCLUDED" in sql
    assert "DELETE FROM" not in sql
    assert "'falsifier_seed'" in sql


def test_validation_rejects_missing_falsifier_member() -> None:
    raw = _valid_raw_config()
    raw["universe_memberships"] = [
        membership
        for membership in raw["universe_memberships"]
        if membership["ticker"] != "JPM"
    ]

    with pytest.raises(SeedValidationError, match="must contain exactly"):
        validate_seed_config(raw)


def test_validation_rejects_unknown_membership_ticker() -> None:
    raw = _valid_raw_config()
    raw["universe_memberships"][0]["ticker"] = "SPY"

    with pytest.raises(SeedValidationError, match="unknown security ticker"):
        validate_seed_config(raw)


def test_validation_rejects_invalid_membership_date_range() -> None:
    raw = _valid_raw_config()
    raw["universe_memberships"][0]["valid_to"] = "2014-04-02"

    with pytest.raises(SeedValidationError, match="valid_to must be on or after"):
        validate_seed_config(raw)


def test_check_command_fails_fast_on_bad_universe_membership(tmp_path: Path) -> None:
    config_path = tmp_path / "seed_reference_data.yaml"
    config_path.write_text(
        textwrap.dedent(
            """
            seed_set_version: 1
            securities:
              - ticker: NVDA
                name: NVIDIA Corporation
                cik: "0001045810"
                exchange: NASDAQ
                identifiers:
                  - type: ticker
                    identifier: NVDA
                    valid_from: "1999-01-22"
                  - type: cik
                    identifier: "0001045810"
                    valid_from: "1999-01-22"
            universe_memberships:
              - universe_name: falsifier_seed
                ticker: NVDA
                valid_from: "2014-04-03"
            """
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(SEED_SCRIPT),
            "--check",
            "--config-path",
            str(config_path),
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "must contain exactly" in result.stderr


def _valid_raw_config() -> dict:
    seed_config = load_seed_file(DEFAULT_CONFIG_PATH)
    return {
        "seed_set_version": 1,
        "securities": [
            {
                "ticker": security.ticker,
                "name": security.name,
                "cik": security.cik,
                "exchange": security.exchange,
                "asset_class": security.asset_class,
                "country": security.country,
                "currency": security.currency,
                "fiscal_year_end_md": security.fiscal_year_end_md,
                "listed_at": (
                    security.listed_at.isoformat()
                    if security.listed_at is not None
                    else None
                ),
                "delisted_at": (
                    security.delisted_at.isoformat()
                    if security.delisted_at is not None
                    else None
                ),
                "identifiers": [
                    {
                        "type": identifier.identifier_type,
                        "identifier": identifier.identifier,
                        "valid_from": identifier.valid_from.isoformat(),
                        "valid_to": (
                            identifier.valid_to.isoformat()
                            if identifier.valid_to is not None
                            else None
                        ),
                    }
                    for identifier in security.identifiers
                ],
            }
            for security in seed_config.securities
        ],
        "universe_memberships": [
            {
                "universe_name": membership.universe_name,
                "ticker": membership.ticker,
                "valid_from": membership.valid_from.isoformat(),
                "valid_to": (
                    membership.valid_to.isoformat()
                    if membership.valid_to is not None
                    else None
                ),
                "reason": membership.reason,
            }
            for membership in seed_config.universe_memberships
        ],
    }
