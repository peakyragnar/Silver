from __future__ import annotations

import importlib.util
import sys
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from silver.features import (
    DEFAULT_CANDIDATE_CONFIG_PATH,
    FeatureCandidate,
    FeatureStoreError,
    FeatureValueWrite,
    feature_candidate_by_key,
    feature_candidate_keys,
    feature_definition_hash,
    load_feature_candidates,
    materialize_feature_candidate,
)
from silver.features.dollar_volume import AdjustedPriceVolumeObservation
from silver.features.momentum_12_1 import (
    AdjustedDailyPriceObservation,
    daily_price_available_at,
)
from silver.features.repository import (
    AvailableAtPolicyRecord,
    FeatureDefinitionRecord,
    UniverseMembershipRecord,
)
from silver.time.trading_calendar import TradingCalendarRow


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run_feature_candidate_pack.py"
SECURITY_ID = 101


def load_candidate_pack_cli():
    spec = importlib.util.spec_from_file_location(
        "run_feature_candidate_pack",
        SCRIPT,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


candidate_pack_cli = load_candidate_pack_cli()


def test_feature_candidate_pack_v1_keys_are_stable() -> None:
    assert feature_candidate_keys() == (
        "momentum_12_1",
        "avg_dollar_volume_63",
        "momentum_6_1",
        "momentum_3_0",
        "short_reversal_21_0",
        "low_realized_volatility_63",
    )


def test_default_feature_candidate_config_loads_pack_metadata() -> None:
    candidates = load_feature_candidates(DEFAULT_CANDIDATE_CONFIG_PATH)

    assert candidates[0].candidate_pack_key == "numeric_feature_pack_v1"
    assert candidates[0].hypothesis_key == "momentum_12_1"
    assert candidates[0].signal_name == "momentum_12_1"
    assert candidates[0].materializer == "momentum_12_1"


def test_load_feature_candidates_rejects_source_feature_mismatch(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "feature_candidates.yaml"
    config_path.write_text(
        """
version: 1
candidate_pack: test_pack
candidates:
  - hypothesis_key: bad_volume
    name: Bad Volume
    source_feature: momentum_12_1
    materializer: avg_dollar_volume_63
    selection_direction: high
    thesis: Bad source feature.
    mechanism: Bad mechanism.
    notes: Bad notes.
""".lstrip(),
        encoding="utf-8",
    )

    try:
        load_feature_candidates(config_path)
    except FeatureStoreError as exc:
        assert "source_feature momentum_12_1 does not match" in str(exc)
    else:  # pragma: no cover - assertion branch.
        raise AssertionError("expected source feature mismatch to fail")


def test_materialize_avg_dollar_volume_candidate_writes_feature_value() -> None:
    candidate = feature_candidate_by_key("avg_dollar_volume_63")
    calendar_rows, sessions = _calendar_rows(date(2024, 1, 2), session_count=63)
    repository = FakeCandidateRepository(
        candidate=candidate,
        calendar_rows=calendar_rows,
        price_volumes={
            SECURITY_ID: [
                _price_volume(session, adjusted_close="10.00", volume=100)
                for session in sessions
            ]
        },
    )

    summary = materialize_feature_candidate(
        repository,
        candidate,
        universe_name="falsifier_seed",
        start_date=None,
        end_date=None,
        computed_by_run_id=88,
    )

    assert summary.values_written == 1
    assert summary.skipped_by_reason == {"insufficient_history": 62}
    [stored] = repository.feature_values.values()
    assert stored.value == 1000.0
    assert stored.asof_date == sessions[-1]
    assert stored.source_metadata["candidate_key"] == "avg_dollar_volume_63"
    assert stored.source_metadata["selection_direction"] == "high"
    assert stored.source_metadata["window"]["observation_count"] == 63


def test_materialize_low_realized_volatility_candidate_writes_feature_value() -> None:
    candidate = feature_candidate_by_key("low_realized_volatility_63")
    calendar_rows, sessions = _calendar_rows(date(2024, 1, 2), session_count=64)
    repository = FakeCandidateRepository(
        candidate=candidate,
        calendar_rows=calendar_rows,
        prices={
            SECURITY_ID: [
                _price(session, Decimal("100") + Decimal(index))
                for index, session in enumerate(sessions)
            ]
        },
    )

    summary = materialize_feature_candidate(
        repository,
        candidate,
        universe_name="falsifier_seed",
        start_date=None,
        end_date=None,
        computed_by_run_id=89,
    )

    assert summary.values_written == 1
    assert summary.skipped_by_reason == {"insufficient_history": 63}
    [stored] = repository.feature_values.values()
    assert stored.value > 0
    assert stored.asof_date == sessions[-1]
    assert stored.source_metadata["candidate_key"] == "low_realized_volatility_63"
    assert stored.source_metadata["selection_direction"] == "low"
    assert stored.source_metadata["window"]["return_count"] == 63


def test_materialize_short_reversal_candidate_writes_price_return_value() -> None:
    candidate = feature_candidate_by_key("short_reversal_21_0")
    calendar_rows, sessions = _calendar_rows(date(2024, 1, 2), session_count=22)
    repository = FakeCandidateRepository(
        candidate=candidate,
        calendar_rows=calendar_rows,
        prices={
            SECURITY_ID: [
                _price(sessions[0], Decimal("100.00")),
                _price(sessions[-1], Decimal("90.00")),
            ]
        },
    )

    summary = materialize_feature_candidate(
        repository,
        candidate,
        universe_name="falsifier_seed",
        start_date=None,
        end_date=None,
        computed_by_run_id=90,
    )

    assert summary.values_written == 1
    assert summary.skipped_by_reason == {"insufficient_history": 21}
    [stored] = repository.feature_values.values()
    assert stored.value == -0.1
    assert stored.asof_date == sessions[-1]
    assert stored.source_metadata["candidate_key"] == "short_reversal_21_0"
    assert stored.source_metadata["selection_direction"] == "low"
    assert stored.source_metadata["window"]["lookback_sessions"] == 21
    assert stored.source_metadata["window"]["skip_recent_sessions"] == 0


def test_candidate_pack_builds_low_direction_falsifier_command() -> None:
    candidate = feature_candidate_by_key("low_realized_volatility_63")

    command = candidate_pack_cli._falsifier_command(
        candidate,
        universe="falsifier_seed",
        horizon=63,
        output_path=Path("reports/falsifier/candidate_pack/low_vol.md"),
    )

    assert "--strategy" in command
    assert "--database-url" not in command
    assert "realized_volatility_63" in command
    assert command[-2:] == ["--selection-direction", "low"]


def test_candidate_pack_materialize_command_uses_candidate_config() -> None:
    candidate = feature_candidate_by_key("avg_dollar_volume_63")

    command = candidate_pack_cli._materialize_command(
        candidate,
        universe="falsifier_seed",
        candidate_config_path=Path("/tmp/feature_candidates.yaml"),
    )

    assert "--candidate-config" in command
    assert "/tmp/feature_candidates.yaml" in command
    assert command[-2:] == ["--candidate", "avg_dollar_volume_63"]


def test_candidate_hypothesis_carries_feature_and_direction_metadata() -> None:
    candidate = feature_candidate_by_key("low_realized_volatility_63")

    hypothesis = candidate_pack_cli.candidate_hypothesis(
        candidate,
        universe="falsifier_seed",
        horizon=63,
    )

    assert hypothesis.hypothesis_key == "low_realized_volatility_63"
    assert hypothesis.signal_name == "realized_volatility_63"
    assert hypothesis.metadata == {
        "candidate_pack": "numeric_feature_pack_v1",
        "feature": "realized_volatility_63",
        "selection_direction": "low",
    }


def test_parse_backtest_run_id_from_falsifier_output() -> None:
    assert (
        candidate_pack_cli.parse_backtest_run_id(
            "OK: wrote report with status succeeded; model_run_id=9; "
            "backtest_run_id=12"
        )
        == 12
    )


def test_candidate_pack_subprocess_uses_database_url_environment(
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    class Result:
        returncode = 0
        stdout = "OK"
        stderr = ""

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs["env"]
        return Result()

    monkeypatch.setattr(candidate_pack_cli.subprocess, "run", fake_run)

    stdout = candidate_pack_cli._run_command(
        ["python", "scripts/run_falsifier.py"],
        database_url="postgresql://user:pass@localhost/silver",
    )

    assert stdout == "OK"
    assert "postgresql://user:pass@localhost/silver" not in captured["command"]
    assert (
        captured["env"]["DATABASE_URL"]
        == "postgresql://user:pass@localhost/silver"
    )


def _calendar_rows(
    start: date,
    *,
    session_count: int,
) -> tuple[tuple[TradingCalendarRow, ...], list[date]]:
    rows: list[TradingCalendarRow] = []
    sessions: list[date] = []
    current = start
    while len(sessions) < session_count:
        is_session = current.weekday() < 5
        rows.append(
            TradingCalendarRow(
                date=current,
                is_session=is_session,
                session_close=_session_close(current) if is_session else None,
            )
        )
        if is_session:
            sessions.append(current)
        current += timedelta(days=1)
    return tuple(rows), sessions


def _session_close(day: date) -> datetime:
    return datetime.combine(day, time(21), tzinfo=timezone.utc)


def _price(price_date: date, adjusted_close: Decimal) -> AdjustedDailyPriceObservation:
    return AdjustedDailyPriceObservation(
        price_date=price_date,
        adjusted_close=adjusted_close,
        available_at=daily_price_available_at(price_date).astimezone(timezone.utc),
    )


def _price_volume(
    price_date: date,
    *,
    adjusted_close: str,
    volume: int,
) -> AdjustedPriceVolumeObservation:
    return AdjustedPriceVolumeObservation(
        price_date=price_date,
        adjusted_close=Decimal(adjusted_close),
        volume=volume,
        available_at=daily_price_available_at(price_date).astimezone(timezone.utc),
    )


class FakeCandidateRepository:
    def __init__(
        self,
        *,
        candidate: FeatureCandidate,
        calendar_rows: tuple[TradingCalendarRow, ...],
        prices: dict[int, list[AdjustedDailyPriceObservation]] | None = None,
        price_volumes: dict[int, list[AdjustedPriceVolumeObservation]] | None = None,
    ) -> None:
        self.definition = FeatureDefinitionRecord(
            id=502,
            name=candidate.definition.name,
            version=candidate.definition.version,
            kind="numeric",
            computation_spec=dict(candidate.definition.computation_spec),
            definition_hash=feature_definition_hash(candidate.definition),
            notes=None,
        )
        self.policy = AvailableAtPolicyRecord(
            id=3,
            name="daily_price",
            version=1,
            rule={
                "type": "date_at_time",
                "base": "price_date",
                "time": "18:00",
                "timezone": "America/New_York",
            },
        )
        self.memberships = (
            UniverseMembershipRecord(
                security_id=SECURITY_ID,
                ticker="AAPL",
                valid_from=date(2020, 1, 1),
                valid_to=None,
            ),
        )
        self.calendar_rows = calendar_rows
        self.prices = prices or {}
        self.price_volumes = price_volumes or {}
        self.feature_values: dict[tuple[int, date, int], FeatureValueWrite] = {}

    def ensure_feature_definition(
        self,
        definition: object,
        *,
        notes: str | None = None,
    ) -> FeatureDefinitionRecord:
        return self.definition

    def load_available_at_policy(
        self,
        *,
        name: str,
        version: int,
    ) -> AvailableAtPolicyRecord:
        return self.policy

    def load_universe_memberships(
        self,
        *,
        universe_name: str,
        start_date: date | None,
        end_date: date | None,
    ) -> tuple[UniverseMembershipRecord, ...]:
        return self.memberships

    def load_trading_calendar(
        self,
        *,
        end_date: date | None,
    ) -> tuple[TradingCalendarRow, ...]:
        if end_date is None:
            return self.calendar_rows
        return tuple(row for row in self.calendar_rows if row.date <= end_date)

    def load_adjusted_prices(
        self,
        *,
        security_ids: list[int] | tuple[int, ...],
        end_date: date | None,
        available_at_policy_id: int,
    ) -> tuple[tuple[int, AdjustedDailyPriceObservation], ...]:
        rows: list[tuple[int, AdjustedDailyPriceObservation]] = []
        for security_id in security_ids:
            for price in self.prices.get(security_id, []):
                if end_date is None or price.price_date <= end_date:
                    rows.append((security_id, price))
        return tuple(rows)

    def load_adjusted_price_volumes(
        self,
        *,
        security_ids: list[int] | tuple[int, ...],
        end_date: date | None,
        available_at_policy_id: int,
    ) -> tuple[tuple[int, AdjustedPriceVolumeObservation], ...]:
        rows: list[tuple[int, AdjustedPriceVolumeObservation]] = []
        for security_id in security_ids:
            for price in self.price_volumes.get(security_id, []):
                if end_date is None or price.price_date <= end_date:
                    rows.append((security_id, price))
        return tuple(rows)

    def write_feature_values(self, values: Any) -> object:
        for value in values:
            key = (value.security_id, value.asof_date, value.feature_definition_id)
            self.feature_values[key] = value
        return object()
