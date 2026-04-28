"""Load, validate, and seed Silver available_at policy definitions."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_PATH = ROOT / "config" / "available_at_policies.yaml"
DEFAULT_VALID_FROM = "1970-01-01T00:00:00+00:00"
REQUIRED_POLICY_NAMES = frozenset(
    {
        "daily_price",
        "sec_10k_filing",
        "sec_10q_filing",
        "sec_8k_material",
        "earnings_call_transcript",
        "press_release_timestamped",
        "press_release_date_only",
        "fundamental_fact_from_filing",
        "fmp_profile_static_data",
        "corporate_action",
        "news_timestamped",
        "xbrl_companyfacts",
    }
)
RULE_REQUIRED_KEYS = {
    "date_at_time": ("base", "time", "timezone"),
    "fetched_at": ("base",),
    "inherit_source_available_at": ("source",),
    "next_trading_session_time_after_date": (
        "base",
        "trading_days_offset",
        "time",
        "timezone",
    ),
    "next_trading_session_time_after_timestamp": (
        "base",
        "trading_days_offset",
        "time",
        "timezone",
    ),
    "timestamp_plus_duration": ("base", "duration"),
    "timestamp_plus_duration_with_fallback": (
        "base",
        "duration",
        "fallback_base",
        "fallback_duration",
    ),
}
TIME_RE = re.compile(r"^\d{2}:\d{2}$")


class PolicyValidationError(ValueError):
    """Raised when a policy file is missing, duplicate, or malformed."""


class PolicySeedError(RuntimeError):
    """Raised when database seeding fails after validation succeeds."""


@dataclass(frozen=True)
class AvailableAtPolicy:
    name: str
    version: int
    rule: Mapping[str, Any]
    valid_from: str = DEFAULT_VALID_FROM
    valid_to: str | None = None
    notes: str | None = None


def load_policy_file(path: Path = DEFAULT_CONFIG_PATH) -> list[AvailableAtPolicy]:
    """Read and validate an available_at policy config file."""
    if not path.exists():
        raise PolicyValidationError(f"policy config does not exist: {path}")

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise PolicyValidationError(f"invalid YAML in policy config: {exc}") from exc

    return validate_policy_config(raw)


def validate_policy_config(raw: object) -> list[AvailableAtPolicy]:
    """Validate parsed policy config data and return normalized policies."""
    if not isinstance(raw, Mapping):
        raise PolicyValidationError("policy config must be a mapping")
    if raw.get("policy_set_version") != 1:
        raise PolicyValidationError("policy_set_version must be 1")

    raw_policies = raw.get("policies")
    if not isinstance(raw_policies, list):
        raise PolicyValidationError("policies must be a list")

    seen: set[tuple[str, int]] = set()
    policies: list[AvailableAtPolicy] = []
    for index, raw_policy in enumerate(raw_policies, start=1):
        policy = _validate_policy(raw_policy, index)
        key = (policy.name, policy.version)
        if key in seen:
            raise PolicyValidationError(
                f"duplicate available_at policy definition: {policy.name} v{policy.version}"
            )
        seen.add(key)
        policies.append(policy)

    version_1_names = {policy.name for policy in policies if policy.version == 1}
    missing = sorted(REQUIRED_POLICY_NAMES - version_1_names)
    if missing:
        raise PolicyValidationError(
            "missing required available_at policy definition(s): "
            + ", ".join(missing)
        )

    return sorted(policies, key=lambda policy: (policy.name, policy.version))


def canonical_rule_json(rule: Mapping[str, Any]) -> str:
    """Return the stable JSON representation used for rule storage."""
    return json.dumps(rule, sort_keys=True, separators=(",", ":"))


def build_upsert_sql(policies: Sequence[AvailableAtPolicy]) -> str:
    """Build deterministic idempotent SQL for silver.available_at_policies."""
    if not policies:
        raise PolicyValidationError("at least one policy is required")

    values = [
        _policy_values_sql(policy)
        for policy in sorted(policies, key=lambda item: (item.name, item.version))
    ]
    values_sql = ",\n    ".join(values)
    return f"""
INSERT INTO silver.available_at_policies
    (name, version, rule, valid_from, valid_to, notes)
VALUES
    {values_sql}
ON CONFLICT (name, version) DO UPDATE SET
    rule = EXCLUDED.rule,
    valid_from = EXCLUDED.valid_from,
    valid_to = EXCLUDED.valid_to,
    notes = EXCLUDED.notes
WHERE
    silver.available_at_policies.rule IS DISTINCT FROM EXCLUDED.rule
    OR silver.available_at_policies.valid_from IS DISTINCT FROM EXCLUDED.valid_from
    OR silver.available_at_policies.valid_to IS DISTINCT FROM EXCLUDED.valid_to
    OR silver.available_at_policies.notes IS DISTINCT FROM EXCLUDED.notes;
""".strip()


def seed_policies(
    policies: Sequence[AvailableAtPolicy],
    database_url: str,
    *,
    psql_path: str | None = None,
) -> None:
    """Insert/update policies through psql after local validation."""
    psql = psql_path or shutil.which("psql")
    if psql is None:
        raise PolicySeedError("psql is required to seed available_at policies")

    result = subprocess.run(
        [psql, "-X", "-v", "ON_ERROR_STOP=1", "-q", "-d", database_url],
        input=build_upsert_sql(policies),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.replace(database_url, "[DATABASE_URL]").strip()
        detail = f": {stderr}" if stderr else ""
        raise PolicySeedError(f"psql failed with exit code {result.returncode}{detail}")


def _validate_policy(raw_policy: object, index: int) -> AvailableAtPolicy:
    if not isinstance(raw_policy, Mapping):
        raise PolicyValidationError(f"policy #{index} must be a mapping")

    name = _required_str(raw_policy, "name", index)
    version = raw_policy.get("version")
    if not isinstance(version, int) or version <= 0:
        raise PolicyValidationError(f"policy {name} must have a positive integer version")

    rule = raw_policy.get("rule")
    if not isinstance(rule, Mapping):
        raise PolicyValidationError(f"policy {name} must have a rule mapping")
    _validate_rule(name, rule)

    valid_from = raw_policy.get("valid_from", DEFAULT_VALID_FROM)
    if not isinstance(valid_from, str):
        raise PolicyValidationError(f"policy {name} valid_from must be a string")
    _parse_timestamp(name, "valid_from", valid_from)

    valid_to = raw_policy.get("valid_to")
    if valid_to is not None:
        if not isinstance(valid_to, str):
            raise PolicyValidationError(f"policy {name} valid_to must be a string or null")
        if _parse_timestamp(name, "valid_to", valid_to) <= _parse_timestamp(
            name,
            "valid_from",
            valid_from,
        ):
            raise PolicyValidationError(f"policy {name} valid_to must be after valid_from")

    notes = raw_policy.get("notes")
    if notes is not None and not isinstance(notes, str):
        raise PolicyValidationError(f"policy {name} notes must be a string or null")

    return AvailableAtPolicy(
        name=name,
        version=version,
        rule=dict(rule),
        valid_from=valid_from,
        valid_to=valid_to,
        notes=notes,
    )


def _validate_rule(name: str, rule: Mapping[str, Any]) -> None:
    rule_type = rule.get("type")
    if not isinstance(rule_type, str) or not rule_type:
        raise PolicyValidationError(f"policy {name} rule.type must be a non-empty string")

    required_keys = RULE_REQUIRED_KEYS.get(rule_type)
    if required_keys is None:
        raise PolicyValidationError(f"policy {name} has unknown rule type {rule_type}")
    missing = [key for key in required_keys if key not in rule]
    if missing:
        raise PolicyValidationError(
            f"policy {name} rule is missing required key(s): {', '.join(missing)}"
        )

    if "time" in rule and (
        not isinstance(rule["time"], str) or TIME_RE.fullmatch(rule["time"]) is None
    ):
        raise PolicyValidationError(f"policy {name} rule.time must use HH:MM format")
    if "trading_days_offset" in rule and (
        not isinstance(rule["trading_days_offset"], int)
        or rule["trading_days_offset"] < 0
    ):
        raise PolicyValidationError(
            f"policy {name} rule.trading_days_offset must be a non-negative integer"
        )

    try:
        canonical_rule_json(rule)
    except TypeError as exc:
        raise PolicyValidationError(f"policy {name} rule must be JSON serializable") from exc


def _required_str(raw_policy: Mapping[str, object], field: str, index: int) -> str:
    value = raw_policy.get(field)
    if not isinstance(value, str) or not value:
        raise PolicyValidationError(f"policy #{index} must have a non-empty {field}")
    return value


def _parse_timestamp(name: str, field: str, value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise PolicyValidationError(
            f"policy {name} {field} must be an ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None:
        raise PolicyValidationError(f"policy {name} {field} must include a timezone")
    return parsed


def _policy_values_sql(policy: AvailableAtPolicy) -> str:
    return (
        "("
        f"{_sql_literal(policy.name)}, "
        f"{policy.version}, "
        f"{_sql_literal(canonical_rule_json(policy.rule))}::jsonb, "
        f"{_sql_literal(policy.valid_from)}::timestamptz, "
        f"{_nullable_timestamp(policy.valid_to)}, "
        f"{_nullable_text(policy.notes)}"
        ")"
    )


def _nullable_timestamp(value: str | None) -> str:
    if value is None:
        return "NULL"
    return f"{_sql_literal(value)}::timestamptz"


def _nullable_text(value: str | None) -> str:
    if value is None:
        return "NULL"
    return _sql_literal(value)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
