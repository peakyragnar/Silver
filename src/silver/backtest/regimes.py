"""Manual Phase 1 regime slicing utilities for backtest analysis.

The default regimes below are explicit calendar-era splits for concentration
analysis only. They are not a macro model, do not infer market state from data,
and must not be used as predictive features without a separate PIT-safe feature
definition.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Generic, TypeVar


T = TypeVar("T")
DateGetter = Callable[[T], date | datetime]
ValueGetter = Callable[[T], float | int | None]


class RegimeDefinitionError(ValueError):
    """Raised when manual regime definitions are ambiguous or invalid."""


class RegimeSliceError(ValueError):
    """Raised when rows cannot be assigned to the configured regimes."""


@dataclass(frozen=True, slots=True)
class RegimeDefinition:
    """One explicit inclusive calendar range used for Phase 1 era slicing."""

    name: str
    start_date: date
    end_date: date
    description: str

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise RegimeDefinitionError("regime name must be non-empty")
        _validate_definition_date(self.start_date, f"{self.name}.start_date")
        _validate_definition_date(self.end_date, f"{self.name}.end_date")
        if self.end_date < self.start_date:
            raise RegimeDefinitionError(
                f"{self.name} end_date must be on or after start_date"
            )
        if not isinstance(self.description, str) or not self.description.strip():
            raise RegimeDefinitionError(f"{self.name} description must be non-empty")

    def contains(self, value: date | datetime) -> bool:
        """Return whether ``value`` lands inside this inclusive date range."""

        day = _coerce_row_date(value, "value")
        return self.start_date <= day <= self.end_date


@dataclass(frozen=True, slots=True)
class RegimeSlice(Generic[T]):
    """Rows assigned to one explicit regime, including empty regimes."""

    regime_name: str
    start_date: date
    end_date: date
    description: str
    rows: tuple[T, ...]

    @property
    def sample_count(self) -> int:
        """Number of rows assigned to this regime."""

        return len(self.rows)


@dataclass(frozen=True, slots=True)
class RegimeSummary:
    """Per-regime sample counts and simple finite-value summary statistics."""

    regime_name: str
    start_date: date
    end_date: date
    description: str
    sample_count: int
    value_count: int
    mean: float | None
    sample_stddev: float | None
    minimum: float | None
    maximum: float | None
    hit_rate: float | None


def validate_regime_definitions(
    regimes: Sequence[RegimeDefinition],
    *,
    require_contiguous: bool = False,
) -> tuple[RegimeDefinition, ...]:
    """Return regimes in deterministic date order after validation.

    Custom regime inputs may arrive in any order. Validation sorts by
    ``start_date``, then ``end_date``, then ``name``; rejects duplicate names;
    and rejects overlapping inclusive date ranges.
    """

    if isinstance(regimes, (str, bytes)) or not isinstance(regimes, Sequence):
        raise RegimeDefinitionError("regimes must be a sequence of RegimeDefinition")

    ordered = tuple(sorted(regimes, key=lambda item: _regime_sort_key(item)))
    if not ordered:
        raise RegimeDefinitionError("at least one regime definition is required")

    seen_names: set[str] = set()
    for index, regime in enumerate(ordered, start=1):
        if not isinstance(regime, RegimeDefinition):
            raise RegimeDefinitionError(
                f"regimes[{index}] must be a RegimeDefinition"
            )
        if regime.name in seen_names:
            raise RegimeDefinitionError(f"duplicate regime name: {regime.name}")
        seen_names.add(regime.name)

    for previous, current in zip(ordered, ordered[1:]):
        if current.start_date <= previous.end_date:
            raise RegimeDefinitionError(
                "regime date ranges must not overlap: "
                f"{previous.name} and {current.name}"
            )
        expected_next_start = previous.end_date + timedelta(days=1)
        if require_contiguous and current.start_date != expected_next_start:
            raise RegimeDefinitionError(
                "regime date ranges must be contiguous: "
                f"{previous.name} ends {previous.end_date.isoformat()} but "
                f"{current.name} starts {current.start_date.isoformat()}"
            )

    return ordered


def default_phase_1_regimes() -> tuple[RegimeDefinition, ...]:
    """Return the manual regimes covering the 2014-2026 seed calendar."""

    return DEFAULT_PHASE_1_REGIMES


def regime_for_date(
    value: date | datetime,
    *,
    regimes: Sequence[RegimeDefinition] | None = None,
) -> RegimeDefinition | None:
    """Return the regime containing ``value``, or ``None`` when uncovered."""

    day = _coerce_row_date(value, "value")
    return _find_regime(day, _validated_or_default(regimes))


def slice_rows_by_regime(
    rows: Iterable[T],
    *,
    date_getter: DateGetter[T],
    regimes: Sequence[RegimeDefinition] | None = None,
) -> tuple[RegimeSlice[T], ...]:
    """Assign rows to explicit regimes and include empty regime slices.

    Rows are processed in input order, while output slices always follow the
    deterministic regime date order. A row outside every configured regime is a
    failure because silently dropping it would hide a coverage gap.
    """

    ordered_regimes = _validated_or_default(regimes)
    buckets: dict[str, list[T]] = {regime.name: [] for regime in ordered_regimes}

    for index, row in enumerate(rows, start=1):
        row_date = _row_date(row, date_getter, index)
        regime = _find_regime(row_date, ordered_regimes)
        if regime is None:
            raise RegimeSliceError(
                f"row {index} date {row_date.isoformat()} is outside all "
                "regime definitions"
            )
        buckets[regime.name].append(row)

    return tuple(
        RegimeSlice(
            regime_name=regime.name,
            start_date=regime.start_date,
            end_date=regime.end_date,
            description=regime.description,
            rows=tuple(buckets[regime.name]),
        )
        for regime in ordered_regimes
    )


def rows_for_regime(
    rows: Iterable[T],
    regime_name: str,
    *,
    date_getter: DateGetter[T],
    regimes: Sequence[RegimeDefinition] | None = None,
) -> tuple[T, ...]:
    """Return rows assigned to one regime name, including an empty tuple."""

    if not isinstance(regime_name, str) or not regime_name.strip():
        raise RegimeSliceError("regime_name must be non-empty")

    ordered_regimes = _validated_or_default(regimes)
    if regime_name not in {regime.name for regime in ordered_regimes}:
        allowed = ", ".join(regime.name for regime in ordered_regimes)
        raise RegimeSliceError(
            f"unknown regime_name {regime_name!r}; expected one of: {allowed}"
        )

    for regime_slice in slice_rows_by_regime(
        rows,
        date_getter=date_getter,
        regimes=ordered_regimes,
    ):
        if regime_slice.regime_name == regime_name:
            return regime_slice.rows

    raise AssertionError("validated regime unexpectedly missing from slice output")


def summarize_by_regime(
    rows: Iterable[T],
    *,
    date_getter: DateGetter[T],
    value_getter: ValueGetter[T] | None = None,
    regimes: Sequence[RegimeDefinition] | None = None,
) -> tuple[RegimeSummary, ...]:
    """Return per-regime counts and finite numeric summary statistics.

    ``sample_count`` includes every row assigned to the regime. ``value_count``
    counts only rows where ``value_getter`` returns a finite numeric value; when
    no getter is supplied, value statistics are reported as ``None``.
    """

    slices = slice_rows_by_regime(rows, date_getter=date_getter, regimes=regimes)
    return tuple(
        _summarize_slice(regime_slice, value_getter) for regime_slice in slices
    )


def _summarize_slice(
    regime_slice: RegimeSlice[T],
    value_getter: ValueGetter[T] | None,
) -> RegimeSummary:
    values = _numeric_values(regime_slice.rows, value_getter, regime_slice.regime_name)
    return RegimeSummary(
        regime_name=regime_slice.regime_name,
        start_date=regime_slice.start_date,
        end_date=regime_slice.end_date,
        description=regime_slice.description,
        sample_count=regime_slice.sample_count,
        value_count=len(values),
        mean=_mean(values),
        sample_stddev=_sample_stddev(values),
        minimum=min(values) if values else None,
        maximum=max(values) if values else None,
        hit_rate=_hit_rate(values),
    )


def _numeric_values(
    rows: Sequence[T],
    value_getter: ValueGetter[T] | None,
    regime_name: str,
) -> tuple[float, ...]:
    if value_getter is None:
        return ()

    values: list[float] = []
    for index, row in enumerate(rows, start=1):
        value = value_getter(row)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise RegimeSliceError(
                f"{regime_name} row {index} summary value must be numeric or None"
            )
        numeric_value = float(value)
        if not math.isfinite(numeric_value):
            raise RegimeSliceError(
                f"{regime_name} row {index} summary value must be finite"
            )
        values.append(numeric_value)

    return tuple(values)


def _row_date(row: T, date_getter: DateGetter[T], index: int) -> date:
    try:
        value = date_getter(row)
    except Exception as exc:
        raise RegimeSliceError(f"date_getter failed for row {index}") from exc
    return _coerce_row_date(value, f"row {index} date")


def _coerce_row_date(value: object, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raise RegimeSliceError(f"{field_name} must be a date or datetime")


def _validate_definition_date(value: object, field_name: str) -> None:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise RegimeDefinitionError(f"{field_name} must be a date")


def _validated_or_default(
    regimes: Sequence[RegimeDefinition] | None,
) -> tuple[RegimeDefinition, ...]:
    if regimes is None:
        return DEFAULT_PHASE_1_REGIMES
    return validate_regime_definitions(regimes)


def _find_regime(
    day: date,
    regimes: Sequence[RegimeDefinition],
) -> RegimeDefinition | None:
    for regime in regimes:
        if regime.start_date <= day <= regime.end_date:
            return regime
    return None


def _regime_sort_key(regime: RegimeDefinition) -> tuple[date, date, str]:
    if not isinstance(regime, RegimeDefinition):
        raise RegimeDefinitionError("regimes must contain RegimeDefinition objects")
    return (regime.start_date, regime.end_date, regime.name)


def _mean(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _sample_stddev(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    average = _mean(values)
    assert average is not None
    variance = sum((value - average) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def _hit_rate(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return sum(1 for value in values if value > 0) / len(values)


DEFAULT_PHASE_1_REGIMES = validate_regime_definitions(
    (
        RegimeDefinition(
            name="low_rate_expansion_2014_2015",
            start_date=date(2014, 1, 1),
            end_date=date(2015, 12, 31),
            description=(
                "Manual calendar split for early seed years in the low-rate "
                "post-crisis expansion; analysis scaffolding only."
            ),
        ),
        RegimeDefinition(
            name="late_cycle_expansion_2016_2019",
            start_date=date(2016, 1, 1),
            end_date=date(2019, 12, 31),
            description=(
                "Manual calendar split for late-cycle pre-COVID seed years; "
                "analysis scaffolding only."
            ),
        ),
        RegimeDefinition(
            name="covid_shock_recovery_2020_2021",
            start_date=date(2020, 1, 1),
            end_date=date(2021, 12, 31),
            description=(
                "Manual calendar split for COVID shock and rebound seed years; "
                "analysis scaffolding only."
            ),
        ),
        RegimeDefinition(
            name="inflation_hike_cycle_2022_2023",
            start_date=date(2022, 1, 1),
            end_date=date(2023, 12, 31),
            description=(
                "Manual calendar split for inflation and rate-hiking seed years; "
                "analysis scaffolding only."
            ),
        ),
        RegimeDefinition(
            name="recent_seed_window_2024_2026",
            start_date=date(2024, 1, 1),
            end_date=date(2026, 12, 31),
            description=(
                "Manual calendar split for the current 2014-2026 seed-calendar "
                "extension; analysis scaffolding only."
            ),
        ),
    ),
    require_contiguous=True,
)


__all__ = [
    "DEFAULT_PHASE_1_REGIMES",
    "RegimeDefinition",
    "RegimeDefinitionError",
    "RegimeSlice",
    "RegimeSliceError",
    "RegimeSummary",
    "default_phase_1_regimes",
    "regime_for_date",
    "rows_for_regime",
    "slice_rows_by_regime",
    "summarize_by_regime",
    "validate_regime_definitions",
]
