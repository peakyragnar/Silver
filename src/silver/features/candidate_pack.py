"""Materialize the first deterministic feature-candidate pack."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Literal, Protocol, cast

import yaml

from silver.features.dollar_volume import (
    AVG_DOLLAR_VOLUME_63_DEFINITION,
    AdjustedPriceVolumeObservation,
    compute_avg_dollar_volume_63,
)
from silver.features.momentum_12_1 import (
    DAILY_PRICE_POLICY_NAME,
    DAILY_PRICE_POLICY_VERSION,
    LONG_LOOKBACK_SESSIONS,
    MOMENTUM_12_1_DEFINITION,
    SKIP_RECENT_SESSIONS,
    AdjustedDailyPriceObservation,
    NumericFeatureDefinition,
    compute_momentum_12_1,
    daily_price_available_at,
)
from silver.features.price_return import (
    MOMENTUM_6_1_DEFINITION,
    MOMENTUM_6_1_LOOKBACK_SESSIONS,
    RETURN_21_0_DEFINITION,
    RETURN_21_0_LOOKBACK_SESSIONS,
    RETURN_63_0_DEFINITION,
    RETURN_63_0_LOOKBACK_SESSIONS,
    compute_momentum_6_1,
    compute_return_21_0,
    compute_return_63_0,
)
from silver.features.realized_volatility import (
    REALIZED_VOLATILITY_63_DEFINITION,
    RETURN_WINDOW_SESSIONS,
    compute_realized_volatility_63,
)
from silver.features.repository import (
    AvailableAtPolicyRecord,
    FeatureDefinitionRecord,
    FeatureStoreError,
    FeatureValueWrite,
    UniverseMembershipRecord,
)
from silver.time.trading_calendar import TradingCalendar, TradingCalendarRow


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CANDIDATE_CONFIG_PATH = ROOT / "config" / "feature_candidates.yaml"

SelectionDirection = Literal["high", "low"]
CandidateMaterializer = Literal[
    "momentum_12_1",
    "momentum_6_1",
    "return_63_0",
    "return_21_0",
    "avg_dollar_volume_63",
    "realized_volatility_63",
]


class CandidateFeatureRepository(Protocol):
    def ensure_feature_definition(
        self,
        definition: NumericFeatureDefinition,
        *,
        notes: str | None = None,
    ) -> FeatureDefinitionRecord:
        ...

    def load_available_at_policy(
        self,
        *,
        name: str,
        version: int,
    ) -> AvailableAtPolicyRecord:
        ...

    def load_universe_memberships(
        self,
        *,
        universe_name: str,
        start_date: date | None,
        end_date: date | None,
    ) -> tuple[UniverseMembershipRecord, ...]:
        ...

    def load_trading_calendar(
        self,
        *,
        end_date: date | None,
    ) -> tuple[TradingCalendarRow, ...]:
        ...

    def load_adjusted_prices(
        self,
        *,
        security_ids: Sequence[int],
        end_date: date | None,
        available_at_policy_id: int,
    ) -> tuple[tuple[int, AdjustedDailyPriceObservation], ...]:
        ...

    def load_adjusted_price_volumes(
        self,
        *,
        security_ids: Sequence[int],
        end_date: date | None,
        available_at_policy_id: int,
    ) -> tuple[tuple[int, AdjustedPriceVolumeObservation], ...]:
        ...

    def write_feature_values(
        self,
        values: Sequence[FeatureValueWrite],
    ) -> object:
        ...


@dataclass(frozen=True, slots=True)
class FeatureCandidate:
    candidate_pack_key: str
    hypothesis_key: str
    name: str
    thesis: str
    signal_name: str
    mechanism: str
    definition: NumericFeatureDefinition
    materializer: CandidateMaterializer
    selection_direction: SelectionDirection
    notes: str


_MATERIALIZER_FEATURE_DEFINITIONS: dict[
    CandidateMaterializer,
    NumericFeatureDefinition,
] = {
    "momentum_12_1": MOMENTUM_12_1_DEFINITION,
    "momentum_6_1": MOMENTUM_6_1_DEFINITION,
    "return_63_0": RETURN_63_0_DEFINITION,
    "return_21_0": RETURN_21_0_DEFINITION,
    "avg_dollar_volume_63": AVG_DOLLAR_VOLUME_63_DEFINITION,
    "realized_volatility_63": REALIZED_VOLATILITY_63_DEFINITION,
}


@dataclass(frozen=True, slots=True)
class CandidateMaterializationSummary:
    candidate_key: str
    feature_definition_id: int
    universe_name: str
    requested_start_date: date | None
    requested_end_date: date | None
    materialized_start_date: date | None
    materialized_end_date: date | None
    securities_seen: int
    eligible_security_dates: int
    values_written: int
    skipped_by_reason: Mapping[str, int]

    @property
    def skipped_total(self) -> int:
        return sum(self.skipped_by_reason.values())


def load_feature_candidates(
    config_path: Path | str = DEFAULT_CANDIDATE_CONFIG_PATH,
) -> tuple[FeatureCandidate, ...]:
    path = Path(config_path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FeatureStoreError(f"feature candidate config not found: {path}") from exc
    except yaml.YAMLError as exc:
        raise FeatureStoreError(f"invalid feature candidate YAML in {path}: {exc}") from exc

    if not isinstance(raw, Mapping):
        raise FeatureStoreError("feature candidate config must be a mapping")
    version = raw.get("version")
    if version != 1:
        raise FeatureStoreError("feature candidate config version must be 1")
    candidate_pack_key = _required_text(raw, "candidate_pack")
    raw_candidates = raw.get("candidates")
    if not isinstance(raw_candidates, list) or not raw_candidates:
        raise FeatureStoreError("feature candidate config must include candidates")

    candidates: list[FeatureCandidate] = []
    seen_keys: set[str] = set()
    for index, raw_candidate in enumerate(raw_candidates, start=1):
        if not isinstance(raw_candidate, Mapping):
            raise FeatureStoreError(f"candidate #{index} must be a mapping")
        candidate = _candidate_from_config(
            raw_candidate,
            candidate_pack_key=candidate_pack_key,
            index=index,
        )
        if candidate.hypothesis_key in seen_keys:
            raise FeatureStoreError(
                f"duplicate feature candidate key {candidate.hypothesis_key}"
            )
        seen_keys.add(candidate.hypothesis_key)
        candidates.append(candidate)
    return tuple(candidates)


def feature_candidate_keys(
    candidates: Sequence[FeatureCandidate] | None = None,
) -> tuple[str, ...]:
    source = FEATURE_CANDIDATES if candidates is None else candidates
    return tuple(candidate.hypothesis_key for candidate in source)


def feature_candidate_by_key(
    key: str,
    *,
    candidates: Sequence[FeatureCandidate] | None = None,
) -> FeatureCandidate:
    normalized = _candidate_key(key)
    source = FEATURE_CANDIDATES if candidates is None else candidates
    for candidate in source:
        if candidate.hypothesis_key == normalized:
            return candidate
    allowed = ", ".join(feature_candidate_keys(source))
    raise FeatureStoreError(f"unknown feature candidate {normalized}; choose {allowed}")


def feature_candidates_for_keys(
    keys: Sequence[str] | None,
    *,
    config_path: Path | str | None = None,
) -> tuple[FeatureCandidate, ...]:
    candidates = (
        FEATURE_CANDIDATES
        if config_path is None
        else load_feature_candidates(config_path)
    )
    if not keys:
        return candidates
    return tuple(
        feature_candidate_by_key(key, candidates=candidates)
        for key in keys
    )


def materialize_feature_candidate(
    repository: CandidateFeatureRepository,
    candidate: FeatureCandidate,
    *,
    universe_name: str,
    start_date: date | None,
    end_date: date | None,
    computed_by_run_id: int,
    dry_run: bool = False,
    available_at_cutoff: datetime | None = None,
) -> CandidateMaterializationSummary:
    _validate_date_bounds(start_date=start_date, end_date=end_date)
    if computed_by_run_id <= 0:
        raise FeatureStoreError("computed_by_run_id must be a positive integer")
    cutoff = available_at_cutoff or datetime.now(timezone.utc)
    _require_aware(cutoff, "available_at_cutoff")

    definition = repository.ensure_feature_definition(
        candidate.definition,
        notes=candidate.notes,
    )
    policy = repository.load_available_at_policy(
        name=DAILY_PRICE_POLICY_NAME,
        version=DAILY_PRICE_POLICY_VERSION,
    )
    memberships = repository.load_universe_memberships(
        universe_name=universe_name,
        start_date=start_date,
        end_date=end_date,
    )
    if not memberships:
        raise FeatureStoreError(f"universe {universe_name} has no eligible members")

    security_ids = tuple(sorted({membership.security_id for membership in memberships}))
    calendar_rows = repository.load_trading_calendar(end_date=end_date)
    calendar = TradingCalendar(calendar_rows)
    candidate_dates = _candidate_asof_dates(
        calendar_rows=calendar.rows,
        start_date=start_date,
        end_date=end_date,
        available_at_cutoff=cutoff,
    )

    if candidate.materializer == "avg_dollar_volume_63":
        writes, skipped, eligible = _materialize_dollar_volume(
            repository,
            candidate=candidate,
            definition=definition,
            policy=policy,
            memberships=memberships,
            security_ids=security_ids,
            calendar=calendar,
            candidate_dates=candidate_dates,
            universe_name=universe_name,
            end_date=end_date,
            computed_by_run_id=computed_by_run_id,
        )
    else:
        writes, skipped, eligible = _materialize_price_only_candidate(
            repository,
            candidate=candidate,
            definition=definition,
            policy=policy,
            memberships=memberships,
            security_ids=security_ids,
            calendar=calendar,
            candidate_dates=candidate_dates,
            universe_name=universe_name,
            end_date=end_date,
            computed_by_run_id=computed_by_run_id,
        )

    if not dry_run:
        repository.write_feature_values(writes)

    return CandidateMaterializationSummary(
        candidate_key=candidate.hypothesis_key,
        feature_definition_id=definition.id,
        universe_name=universe_name,
        requested_start_date=start_date,
        requested_end_date=end_date,
        materialized_start_date=candidate_dates[0] if candidate_dates else None,
        materialized_end_date=candidate_dates[-1] if candidate_dates else None,
        securities_seen=len(security_ids),
        eligible_security_dates=eligible,
        values_written=len(writes),
        skipped_by_reason=dict(sorted(skipped.items())),
    )


def _materialize_price_only_candidate(
    repository: CandidateFeatureRepository,
    *,
    candidate: FeatureCandidate,
    definition: FeatureDefinitionRecord,
    policy: AvailableAtPolicyRecord,
    memberships: Sequence[UniverseMembershipRecord],
    security_ids: Sequence[int],
    calendar: TradingCalendar,
    candidate_dates: Sequence[date],
    universe_name: str,
    end_date: date | None,
    computed_by_run_id: int,
) -> tuple[list[FeatureValueWrite], Counter[str], int]:
    prices = repository.load_adjusted_prices(
        security_ids=security_ids,
        end_date=end_date,
        available_at_policy_id=policy.id,
    )
    price_lookup_by_security = _price_lookup_by_security(prices)
    session_dates = tuple(row.date for row in calendar.rows if row.is_session)
    session_index = {session: index for index, session in enumerate(session_dates)}
    writes: list[FeatureValueWrite] = []
    skipped: Counter[str] = Counter()
    eligible_security_dates = 0

    for asof_date in candidate_dates:
        asof = daily_price_available_at(asof_date).astimezone(timezone.utc)
        for membership in memberships:
            if not membership.is_active_on(asof_date):
                continue
            eligible_security_dates += 1
            security_prices = price_lookup_by_security.get(membership.security_id, {})
            if candidate.materializer == "momentum_12_1":
                observations = _momentum_boundary_prices(
                    security_prices=security_prices,
                    session_dates=session_dates,
                    session_index=session_index,
                    asof_date=asof_date,
                )
                if observations is None:
                    skipped["insufficient_history"] += 1
                    continue
                result = compute_momentum_12_1(
                    security_id=membership.security_id,
                    asof=asof,
                    prices=observations,
                    calendar=calendar,
                )
            elif candidate.materializer == "realized_volatility_63":
                observations = _rolling_price_window(
                    security_prices=security_prices,
                    session_dates=session_dates,
                    session_index=session_index,
                    asof_date=asof_date,
                    required_observations=RETURN_WINDOW_SESSIONS + 1,
                )
                if observations is None:
                    skipped["insufficient_history"] += 1
                    continue
                result = compute_realized_volatility_63(
                    security_id=membership.security_id,
                    asof=asof,
                    prices=observations,
                    calendar=calendar,
                )
            elif candidate.materializer in (
                "momentum_6_1",
                "return_63_0",
                "return_21_0",
            ):
                start_offset, end_offset = _price_return_offsets(
                    candidate.materializer
                )
                observations = _price_return_boundary_prices(
                    security_prices=security_prices,
                    session_dates=session_dates,
                    session_index=session_index,
                    asof_date=asof_date,
                    start_offset=start_offset,
                    end_offset=end_offset,
                )
                if observations is None:
                    skipped["insufficient_history"] += 1
                    continue
                if candidate.materializer == "momentum_6_1":
                    result = compute_momentum_6_1(
                        security_id=membership.security_id,
                        asof=asof,
                        prices=observations,
                        calendar=calendar,
                    )
                elif candidate.materializer == "return_63_0":
                    result = compute_return_63_0(
                        security_id=membership.security_id,
                        asof=asof,
                        prices=observations,
                        calendar=calendar,
                    )
                else:
                    result = compute_return_21_0(
                        security_id=membership.security_id,
                        asof=asof,
                        prices=observations,
                        calendar=calendar,
                    )
            else:  # pragma: no cover - guarded by caller branch.
                raise FeatureStoreError(f"unsupported candidate {candidate.materializer}")

            if result.status != "ok" or result.value is None:
                skipped[result.status] += 1
                continue
            writes.append(
                _feature_value_write(
                    result=result,
                    definition=definition,
                    policy=policy,
                    candidate=candidate,
                    universe_name=universe_name,
                    computed_by_run_id=computed_by_run_id,
                )
            )
    return writes, skipped, eligible_security_dates


def _materialize_dollar_volume(
    repository: CandidateFeatureRepository,
    *,
    candidate: FeatureCandidate,
    definition: FeatureDefinitionRecord,
    policy: AvailableAtPolicyRecord,
    memberships: Sequence[UniverseMembershipRecord],
    security_ids: Sequence[int],
    calendar: TradingCalendar,
    candidate_dates: Sequence[date],
    universe_name: str,
    end_date: date | None,
    computed_by_run_id: int,
) -> tuple[list[FeatureValueWrite], Counter[str], int]:
    rows = repository.load_adjusted_price_volumes(
        security_ids=security_ids,
        end_date=end_date,
        available_at_policy_id=policy.id,
    )
    price_lookup_by_security = _price_volume_lookup_by_security(rows)
    session_dates = tuple(row.date for row in calendar.rows if row.is_session)
    session_index = {session: index for index, session in enumerate(session_dates)}
    writes: list[FeatureValueWrite] = []
    skipped: Counter[str] = Counter()
    eligible_security_dates = 0

    for asof_date in candidate_dates:
        asof = daily_price_available_at(asof_date).astimezone(timezone.utc)
        for membership in memberships:
            if not membership.is_active_on(asof_date):
                continue
            eligible_security_dates += 1
            observations = _rolling_price_volume_window(
                security_rows=price_lookup_by_security.get(membership.security_id, {}),
                session_dates=session_dates,
                session_index=session_index,
                asof_date=asof_date,
                required_observations=63,
            )
            if observations is None:
                skipped["insufficient_history"] += 1
                continue
            result = compute_avg_dollar_volume_63(
                security_id=membership.security_id,
                asof=asof,
                observations=observations,
                calendar=calendar,
            )
            if result.status != "ok" or result.value is None:
                skipped[result.status] += 1
                continue
            writes.append(
                _feature_value_write(
                    result=result,
                    definition=definition,
                    policy=policy,
                    candidate=candidate,
                    universe_name=universe_name,
                    computed_by_run_id=computed_by_run_id,
                )
            )
    return writes, skipped, eligible_security_dates


def _feature_value_write(
    *,
    result: object,
    definition: FeatureDefinitionRecord,
    policy: AvailableAtPolicyRecord,
    candidate: FeatureCandidate,
    universe_name: str,
    computed_by_run_id: int,
) -> FeatureValueWrite:
    return FeatureValueWrite(
        security_id=getattr(result, "security_id"),
        asof_date=getattr(result, "asof_date"),
        feature_definition_id=definition.id,
        value=float(getattr(result, "value")),
        available_at=getattr(result, "available_at"),
        available_at_policy_id=policy.id,
        computed_by_run_id=computed_by_run_id,
        source_metadata={
            "source": f"silver.features.{candidate.materializer}",
            "candidate_key": candidate.hypothesis_key,
            "selection_direction": candidate.selection_direction,
            "universe_name": universe_name,
            "available_at": getattr(result, "available_at").isoformat(),
            "daily_price_policy": {"name": policy.name, "version": policy.version},
            "window": _metadata_value(getattr(result, "window")),
        },
    )


def _candidate_asof_dates(
    *,
    calendar_rows: Sequence[TradingCalendarRow],
    start_date: date | None,
    end_date: date | None,
    available_at_cutoff: datetime,
) -> tuple[date, ...]:
    return tuple(
        row.date
        for row in calendar_rows
        if row.is_session
        and (start_date is None or row.date >= start_date)
        and (end_date is None or row.date <= end_date)
        and daily_price_available_at(row.date).astimezone(timezone.utc)
        <= available_at_cutoff
    )


def _momentum_boundary_prices(
    *,
    security_prices: Mapping[date, AdjustedDailyPriceObservation],
    session_dates: Sequence[date],
    session_index: Mapping[date, int],
    asof_date: date,
) -> tuple[AdjustedDailyPriceObservation, ...] | None:
    index = session_index[asof_date]
    if index < LONG_LOOKBACK_SESSIONS:
        return None
    boundary_dates = (
        session_dates[index - LONG_LOOKBACK_SESSIONS],
        session_dates[index - SKIP_RECENT_SESSIONS],
    )
    return tuple(
        price
        for boundary_date in boundary_dates
        for price in [security_prices.get(boundary_date)]
        if price is not None
    )


def _rolling_price_window(
    *,
    security_prices: Mapping[date, AdjustedDailyPriceObservation],
    session_dates: Sequence[date],
    session_index: Mapping[date, int],
    asof_date: date,
    required_observations: int,
) -> tuple[AdjustedDailyPriceObservation, ...] | None:
    index = session_index[asof_date]
    if index < required_observations - 1:
        return None
    window_dates = session_dates[index - required_observations + 1 : index + 1]
    return tuple(
        price
        for window_date in window_dates
        for price in [security_prices.get(window_date)]
        if price is not None
    )


def _price_return_offsets(
    materializer: CandidateMaterializer,
) -> tuple[int, int]:
    if materializer == "momentum_6_1":
        return MOMENTUM_6_1_LOOKBACK_SESSIONS, SKIP_RECENT_SESSIONS
    if materializer == "return_63_0":
        return RETURN_63_0_LOOKBACK_SESSIONS, 0
    if materializer == "return_21_0":
        return RETURN_21_0_LOOKBACK_SESSIONS, 0
    raise FeatureStoreError(f"unsupported price-return materializer {materializer}")


def _price_return_boundary_prices(
    *,
    security_prices: Mapping[date, AdjustedDailyPriceObservation],
    session_dates: Sequence[date],
    session_index: Mapping[date, int],
    asof_date: date,
    start_offset: int,
    end_offset: int,
) -> tuple[AdjustedDailyPriceObservation, ...] | None:
    index = session_index[asof_date]
    if index < start_offset or index < end_offset:
        return None
    boundary_dates = (
        session_dates[index - start_offset],
        session_dates[index - end_offset],
    )
    return tuple(
        price
        for boundary_date in boundary_dates
        for price in [security_prices.get(boundary_date)]
        if price is not None
    )


def _rolling_price_volume_window(
    *,
    security_rows: Mapping[date, AdjustedPriceVolumeObservation],
    session_dates: Sequence[date],
    session_index: Mapping[date, int],
    asof_date: date,
    required_observations: int,
) -> tuple[AdjustedPriceVolumeObservation, ...] | None:
    index = session_index[asof_date]
    if index < required_observations - 1:
        return None
    window_dates = session_dates[index - required_observations + 1 : index + 1]
    return tuple(
        row
        for window_date in window_dates
        for row in [security_rows.get(window_date)]
        if row is not None
    )


def _price_lookup_by_security(
    rows: Sequence[tuple[int, AdjustedDailyPriceObservation]],
) -> dict[int, dict[date, AdjustedDailyPriceObservation]]:
    grouped: defaultdict[int, dict[date, AdjustedDailyPriceObservation]] = defaultdict(
        dict
    )
    for security_id, price in rows:
        grouped[security_id][price.price_date] = price
    return dict(grouped)


def _price_volume_lookup_by_security(
    rows: Sequence[tuple[int, AdjustedPriceVolumeObservation]],
) -> dict[int, dict[date, AdjustedPriceVolumeObservation]]:
    grouped: defaultdict[int, dict[date, AdjustedPriceVolumeObservation]] = defaultdict(
        dict
    )
    for security_id, row in rows:
        grouped[security_id][row.price_date] = row
    return dict(grouped)


def _metadata_value(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if is_dataclass(value):
        return {
            field.name: _metadata_value(getattr(value, field.name))
            for field in fields(value)
            if getattr(value, field.name) not in (None, ())
        }
    if isinstance(value, Mapping):
        return {
            str(key): _metadata_value(item)
            for key, item in value.items()
            if item not in (None, ())
        }
    if isinstance(value, tuple):
        return [_metadata_value(item) for item in value]
    return value


def _candidate_from_config(
    raw: Mapping[object, object],
    *,
    candidate_pack_key: str,
    index: int,
) -> FeatureCandidate:
    materializer_text = _required_text(raw, "materializer")
    if materializer_text not in _MATERIALIZER_FEATURE_DEFINITIONS:
        allowed = ", ".join(sorted(_MATERIALIZER_FEATURE_DEFINITIONS))
        raise FeatureStoreError(
            f"candidate #{index} uses unsupported materializer "
            f"{materializer_text}; choose {allowed}"
        )
    materializer = cast(CandidateMaterializer, materializer_text)
    definition = _MATERIALIZER_FEATURE_DEFINITIONS[materializer]

    source_feature = _required_text(raw, "source_feature")
    if source_feature != definition.name:
        raise FeatureStoreError(
            f"candidate #{index} source_feature {source_feature} does not match "
            f"materializer {materializer} feature {definition.name}"
        )

    direction_text = _required_text(raw, "selection_direction")
    if direction_text not in ("high", "low"):
        raise FeatureStoreError(
            f"candidate #{index} selection_direction must be high or low"
        )

    return FeatureCandidate(
        candidate_pack_key=candidate_pack_key,
        hypothesis_key=_candidate_key(_required_text(raw, "hypothesis_key")),
        name=_required_text(raw, "name"),
        thesis=_required_text(raw, "thesis"),
        signal_name=source_feature,
        mechanism=_required_text(raw, "mechanism"),
        definition=definition,
        materializer=materializer,
        selection_direction=cast(SelectionDirection, direction_text),
        notes=_required_text(raw, "notes"),
    )


def _required_text(raw: Mapping[object, object], field_name: str) -> str:
    value = raw.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise FeatureStoreError(f"{field_name} must be a non-empty string")
    return value.strip()


def _candidate_key(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FeatureStoreError("candidate key must be a non-empty string")
    return value.strip()


def _validate_date_bounds(*, start_date: date | None, end_date: date | None) -> None:
    if start_date is not None and not isinstance(start_date, date):
        raise FeatureStoreError("start_date must be a date")
    if end_date is not None and not isinstance(end_date, date):
        raise FeatureStoreError("end_date must be a date")
    if start_date is not None and end_date is not None and end_date < start_date:
        raise FeatureStoreError("end_date must be on or after start_date")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FeatureStoreError(f"{field_name} must be timezone-aware")


FEATURE_CANDIDATES: tuple[FeatureCandidate, ...] = load_feature_candidates()
