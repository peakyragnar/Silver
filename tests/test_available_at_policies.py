from __future__ import annotations

import copy
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from silver.time.available_at_policies import (
    DEFAULT_CONFIG_PATH,
    REQUIRED_POLICY_NAMES,
    PolicyValidationError,
    build_upsert_sql,
    canonical_rule_json,
    load_policy_file,
    validate_policy_config,
)


ROOT = Path(__file__).resolve().parents[1]
SEED_SCRIPT = ROOT / "scripts" / "seed_available_at_policies.py"


def test_default_policy_config_covers_required_version_1_sources() -> None:
    policies = load_policy_file(DEFAULT_CONFIG_PATH)

    assert {policy.name for policy in policies if policy.version == 1} == REQUIRED_POLICY_NAMES
    assert all(policy.version == 1 for policy in policies)


def test_default_policy_config_has_unique_name_versions() -> None:
    policies = load_policy_file(DEFAULT_CONFIG_PATH)

    keys = [(policy.name, policy.version) for policy in policies]

    assert len(keys) == len(set(keys))


def test_rule_payload_is_stable_json() -> None:
    policies = load_policy_file(DEFAULT_CONFIG_PATH)
    daily_price = next(policy for policy in policies if policy.name == "daily_price")

    assert canonical_rule_json(daily_price.rule) == (
        '{"base":"price_date","calendar":"NYSE","time":"18:00",'
        '"timezone":"America/New_York","type":"date_at_time"}'
    )


def test_validation_rejects_missing_required_policy() -> None:
    raw = _valid_raw_config()
    raw["policies"] = [
        policy for policy in raw["policies"] if policy["name"] != "news_timestamped"
    ]

    with pytest.raises(PolicyValidationError, match="missing required"):
        validate_policy_config(raw)


def test_validation_rejects_duplicate_policy_definition() -> None:
    raw = _valid_raw_config()
    raw["policies"].append(copy.deepcopy(raw["policies"][0]))

    with pytest.raises(PolicyValidationError, match="duplicate"):
        validate_policy_config(raw)


def test_validation_rejects_malformed_policy_rule() -> None:
    raw = _valid_raw_config()
    raw["policies"][0]["rule"].pop("time")

    with pytest.raises(PolicyValidationError, match="missing required key"):
        validate_policy_config(raw)


def test_load_policy_file_rejects_invalid_yaml(tmp_path: Path) -> None:
    config_path = tmp_path / "available_at_policies.yaml"
    config_path.write_text("policies: [", encoding="utf-8")

    with pytest.raises(PolicyValidationError, match="invalid YAML"):
        load_policy_file(config_path)


def test_build_upsert_sql_is_idempotent_upsert() -> None:
    policies = load_policy_file(DEFAULT_CONFIG_PATH)

    sql = build_upsert_sql(policies)

    assert "ON CONFLICT (name, version) DO UPDATE SET" in sql
    assert "silver.available_at_policies.rule IS DISTINCT FROM EXCLUDED.rule" in sql
    assert "'daily_price'" in sql
    assert "'xbrl_companyfacts'" in sql


def test_check_command_fails_fast_on_missing_required_policy(tmp_path: Path) -> None:
    config_path = tmp_path / "available_at_policies.yaml"
    config_path.write_text(
        textwrap.dedent(
            """
            policy_set_version: 1
            policies:
              - name: daily_price
                version: 1
                valid_from: "1970-01-01T00:00:00+00:00"
                rule:
                  type: date_at_time
                  base: price_date
                  time: "18:00"
                  timezone: America/New_York
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
    assert "missing required available_at policy" in result.stderr


def _valid_raw_config() -> dict:
    return {
        "policy_set_version": 1,
        "policies": [
            {
                "name": policy.name,
                "version": policy.version,
                "valid_from": policy.valid_from,
                "valid_to": policy.valid_to,
                "notes": policy.notes,
                "rule": dict(policy.rule),
            }
            for policy in load_policy_file(DEFAULT_CONFIG_PATH)
        ],
    }
