"""Point-in-time quarterly income-statement feature families."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, DivisionByZero, InvalidOperation
from types import MappingProxyType
from typing import Literal

from silver.features.momentum_12_1 import (
    DAILY_PRICE_TIMEZONE,
    NumericFeatureDefinition,
)


FEATURE_VERSION = 1
STATEMENT_TYPE = "income_statement"
PERIOD_TYPE = "quarterly"
SOURCE_SYSTEM = "fmp"

REVENUE = "revenue"
GROSS_PROFIT = "gross_profit"
OPERATING_INCOME = "operating_income"
NET_INCOME = "net_income"
DILUTED_SHARES = "diluted_weighted_average_shares"

INCOME_STATEMENT_METRICS = (
    REVENUE,
    GROSS_PROFIT,
    OPERATING_INCOME,
    NET_INCOME,
    DILUTED_SHARES,
)

IncomeStatementStatus = Literal[
    "ok",
    "no_visible_period",
    "missing_current_metric",
    "missing_prior_year_metric",
    "zero_denominator",
]


class IncomeStatementFeatureInputError(ValueError):
    """Raised when inputs cannot produce deterministic fundamental features."""


@dataclass(frozen=True, slots=True)
class FundamentalMetricObservation:
    """One normalized fundamental metric plus its point-in-time visibility."""

    id: int
    security_id: int
    period_end_date: date
    fiscal_year: int
    fiscal_period: str
    metric_name: str
    metric_value: Decimal
    available_at: datetime
    available_at_policy_id: int


@dataclass(frozen=True, slots=True)
class QuarterlyIncomeWindow:
    """Fundamental period window selected for a quarterly income feature."""

    current_period_end_date: date | None
    current_fiscal_year: int | None
    current_fiscal_period: str | None
    current_available_at: datetime | None
    prior_period_end_date: date | None = None
    prior_available_at: datetime | None = None
    metric_names: tuple[str, ...] = ()
    source_value_ids: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class QuarterlyIncomeFeatureValue:
    """Typed income-statement feature value ready to map to feature_values."""

    security_id: int
    asof_date: date
    available_at: datetime
    definition: NumericFeatureDefinition
    value: float | None
    status: IncomeStatementStatus
    window: QuarterlyIncomeWindow


REVENUE_GROWTH_YOY_DEFINITION = NumericFeatureDefinition(
    name="revenue_growth_yoy",
    version=FEATURE_VERSION,
    kind="numeric",
    computation_spec=MappingProxyType(
        {
            "statement_type": STATEMENT_TYPE,
            "period_type": PERIOD_TYPE,
            "source_system": SOURCE_SYSTEM,
            "metric": REVENUE,
            "comparison": "same_fiscal_period_prior_year",
            "return_type": "simple_growth",
            "available_at_source": "silver.fundamental_values.available_at",
        }
    ),
)

GROSS_MARGIN_DEFINITION = NumericFeatureDefinition(
    name="gross_margin",
    version=FEATURE_VERSION,
    kind="numeric",
    computation_spec=MappingProxyType(
        {
            "statement_type": STATEMENT_TYPE,
            "period_type": PERIOD_TYPE,
            "source_system": SOURCE_SYSTEM,
            "numerator_metric": GROSS_PROFIT,
            "denominator_metric": REVENUE,
            "available_at_source": "silver.fundamental_values.available_at",
        }
    ),
)

OPERATING_MARGIN_DEFINITION = NumericFeatureDefinition(
    name="operating_margin",
    version=FEATURE_VERSION,
    kind="numeric",
    computation_spec=MappingProxyType(
        {
            "statement_type": STATEMENT_TYPE,
            "period_type": PERIOD_TYPE,
            "source_system": SOURCE_SYSTEM,
            "numerator_metric": OPERATING_INCOME,
            "denominator_metric": REVENUE,
            "available_at_source": "silver.fundamental_values.available_at",
        }
    ),
)

NET_MARGIN_DEFINITION = NumericFeatureDefinition(
    name="net_margin",
    version=FEATURE_VERSION,
    kind="numeric",
    computation_spec=MappingProxyType(
        {
            "statement_type": STATEMENT_TYPE,
            "period_type": PERIOD_TYPE,
            "source_system": SOURCE_SYSTEM,
            "numerator_metric": NET_INCOME,
            "denominator_metric": REVENUE,
            "available_at_source": "silver.fundamental_values.available_at",
        }
    ),
)

DILUTED_SHARES_CHANGE_YOY_DEFINITION = NumericFeatureDefinition(
    name="diluted_shares_change_yoy",
    version=FEATURE_VERSION,
    kind="numeric",
    computation_spec=MappingProxyType(
        {
            "statement_type": STATEMENT_TYPE,
            "period_type": PERIOD_TYPE,
            "source_system": SOURCE_SYSTEM,
            "metric": DILUTED_SHARES,
            "comparison": "same_fiscal_period_prior_year",
            "return_type": "simple_growth",
            "available_at_source": "silver.fundamental_values.available_at",
        }
    ),
)

INCOME_STATEMENT_FEATURE_DEFINITIONS: Mapping[str, NumericFeatureDefinition] = (
    MappingProxyType(
        {
            "revenue_growth_yoy": REVENUE_GROWTH_YOY_DEFINITION,
            "gross_margin": GROSS_MARGIN_DEFINITION,
            "operating_margin": OPERATING_MARGIN_DEFINITION,
            "net_margin": NET_MARGIN_DEFINITION,
            "diluted_shares_change_yoy": DILUTED_SHARES_CHANGE_YOY_DEFINITION,
        }
    )
)


def compute_quarterly_income_feature(
    *,
    security_id: int,
    asof: datetime,
    observations: Sequence[FundamentalMetricObservation],
    definition: NumericFeatureDefinition,
) -> QuarterlyIncomeFeatureValue:
    """Compute a quarterly income feature using only metrics visible at asof."""
    _require_aware(asof, "asof")
    asof_date = asof.astimezone(DAILY_PRICE_TIMEZONE).date()
    visible = tuple(
        observation
        for observation in observations
        if observation.security_id == security_id and observation.available_at <= asof
    )
    if not visible:
        return _null_value(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            definition=definition,
            status="no_visible_period",
            window=QuarterlyIncomeWindow(None, None, None, None),
        )

    period_metrics = _period_metrics(visible)
    ordered_periods = tuple(
        sorted(period_metrics, key=lambda key: (key[2], key[0], key[1]))
    )

    if definition.name == GROSS_MARGIN_DEFINITION.name:
        return _compute_current_ratio(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            definition=definition,
            period_metrics=period_metrics,
            ordered_periods=ordered_periods,
            numerator_metric=GROSS_PROFIT,
            denominator_metric=REVENUE,
        )
    if definition.name == OPERATING_MARGIN_DEFINITION.name:
        return _compute_current_ratio(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            definition=definition,
            period_metrics=period_metrics,
            ordered_periods=ordered_periods,
            numerator_metric=OPERATING_INCOME,
            denominator_metric=REVENUE,
        )
    if definition.name == NET_MARGIN_DEFINITION.name:
        return _compute_current_ratio(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            definition=definition,
            period_metrics=period_metrics,
            ordered_periods=ordered_periods,
            numerator_metric=NET_INCOME,
            denominator_metric=REVENUE,
        )
    if definition.name == REVENUE_GROWTH_YOY_DEFINITION.name:
        return _compute_yoy_growth(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            definition=definition,
            period_metrics=period_metrics,
            ordered_periods=ordered_periods,
            metric_name=REVENUE,
        )
    if definition.name == DILUTED_SHARES_CHANGE_YOY_DEFINITION.name:
        return _compute_yoy_growth(
            security_id=security_id,
            asof=asof,
            asof_date=asof_date,
            definition=definition,
            period_metrics=period_metrics,
            ordered_periods=ordered_periods,
            metric_name=DILUTED_SHARES,
        )
    raise IncomeStatementFeatureInputError(
        f"unsupported quarterly income feature {definition.name}"
    )


def _compute_current_ratio(
    *,
    security_id: int,
    asof: datetime,
    asof_date: date,
    definition: NumericFeatureDefinition,
    period_metrics: Mapping[
        tuple[int, str, date],
        Mapping[str, FundamentalMetricObservation],
    ],
    ordered_periods: Sequence[tuple[int, str, date]],
    numerator_metric: str,
    denominator_metric: str,
) -> QuarterlyIncomeFeatureValue:
    zero_denominator_seen = False
    for key in reversed(ordered_periods):
        metrics = period_metrics[key]
        numerator = metrics.get(numerator_metric)
        denominator = metrics.get(denominator_metric)
        if numerator is None or denominator is None:
            continue
        if denominator.metric_value == 0:
            zero_denominator_seen = True
            continue
        value = _decimal_ratio(numerator.metric_value, denominator.metric_value)
        available_at = max(numerator.available_at, denominator.available_at)
        return QuarterlyIncomeFeatureValue(
            security_id=security_id,
            asof_date=asof_date,
            available_at=available_at,
            definition=definition,
            value=float(value),
            status="ok",
            window=_window(
                current=(
                    numerator
                    if numerator.available_at >= denominator.available_at
                    else denominator
                ),
                prior=None,
                metric_names=(numerator_metric, denominator_metric),
                source_value_ids=(numerator.id, denominator.id),
            ),
        )
    return _null_value(
        security_id=security_id,
        asof=asof,
        asof_date=asof_date,
        definition=definition,
        status="zero_denominator" if zero_denominator_seen else "missing_current_metric",
        window=QuarterlyIncomeWindow(None, None, None, None),
    )


def _compute_yoy_growth(
    *,
    security_id: int,
    asof: datetime,
    asof_date: date,
    definition: NumericFeatureDefinition,
    period_metrics: Mapping[
        tuple[int, str, date],
        Mapping[str, FundamentalMetricObservation],
    ],
    ordered_periods: Sequence[tuple[int, str, date]],
    metric_name: str,
) -> QuarterlyIncomeFeatureValue:
    by_fiscal_period = _latest_period_by_fiscal_period(period_metrics)
    missing_prior_seen = False
    zero_denominator_seen = False
    for key in reversed(ordered_periods):
        current = period_metrics[key].get(metric_name)
        if current is None:
            continue
        prior_key = by_fiscal_period.get((key[0] - 1, key[1]))
        if prior_key is None:
            missing_prior_seen = True
            continue
        prior = period_metrics[prior_key].get(metric_name)
        if prior is None:
            missing_prior_seen = True
            continue
        if prior.metric_value == 0:
            zero_denominator_seen = True
            continue
        value = _decimal_ratio(current.metric_value, prior.metric_value) - Decimal("1")
        available_at = max(current.available_at, prior.available_at)
        return QuarterlyIncomeFeatureValue(
            security_id=security_id,
            asof_date=asof_date,
            available_at=available_at,
            definition=definition,
            value=float(value),
            status="ok",
            window=_window(
                current=current,
                prior=prior,
                metric_names=(metric_name,),
                source_value_ids=(current.id, prior.id),
            ),
        )
    if zero_denominator_seen:
        status: IncomeStatementStatus = "zero_denominator"
    elif missing_prior_seen:
        status = "missing_prior_year_metric"
    else:
        status = "missing_current_metric"
    return _null_value(
        security_id=security_id,
        asof=asof,
        asof_date=asof_date,
        definition=definition,
        status=status,
        window=QuarterlyIncomeWindow(None, None, None, None),
    )


def _period_metrics(
    observations: Sequence[FundamentalMetricObservation],
) -> dict[tuple[int, str, date], dict[str, FundamentalMetricObservation]]:
    grouped: dict[tuple[int, str, date], dict[str, FundamentalMetricObservation]] = (
        defaultdict(dict)
    )
    for observation in observations:
        _validate_observation(observation)
        key = (
            observation.fiscal_year,
            observation.fiscal_period,
            observation.period_end_date,
        )
        grouped[key][observation.metric_name] = observation
    return dict(grouped)


def _latest_period_by_fiscal_period(
    period_metrics: Mapping[tuple[int, str, date], object],
) -> dict[tuple[int, str], tuple[int, str, date]]:
    latest: dict[tuple[int, str], tuple[int, str, date]] = {}
    for key in sorted(period_metrics, key=lambda item: (item[2], item[0], item[1])):
        latest[(key[0], key[1])] = key
    return latest


def _window(
    *,
    current: FundamentalMetricObservation,
    prior: FundamentalMetricObservation | None,
    metric_names: tuple[str, ...],
    source_value_ids: tuple[int, ...],
) -> QuarterlyIncomeWindow:
    return QuarterlyIncomeWindow(
        current_period_end_date=current.period_end_date,
        current_fiscal_year=current.fiscal_year,
        current_fiscal_period=current.fiscal_period,
        current_available_at=current.available_at,
        prior_period_end_date=prior.period_end_date if prior else None,
        prior_available_at=prior.available_at if prior else None,
        metric_names=tuple(sorted(set(metric_names))),
        source_value_ids=tuple(sorted(source_value_ids)),
    )


def _decimal_ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    try:
        return numerator / denominator
    except (DivisionByZero, InvalidOperation) as exc:
        raise IncomeStatementFeatureInputError("invalid decimal ratio") from exc


def _null_value(
    *,
    security_id: int,
    asof: datetime,
    asof_date: date,
    definition: NumericFeatureDefinition,
    status: IncomeStatementStatus,
    window: QuarterlyIncomeWindow,
) -> QuarterlyIncomeFeatureValue:
    return QuarterlyIncomeFeatureValue(
        security_id=security_id,
        asof_date=asof_date,
        available_at=asof,
        definition=definition,
        value=None,
        status=status,
        window=window,
    )


def _validate_observation(observation: FundamentalMetricObservation) -> None:
    if observation.security_id <= 0:
        raise IncomeStatementFeatureInputError("security_id must be positive")
    if observation.fiscal_period not in {"Q1", "Q2", "Q3", "Q4"}:
        raise IncomeStatementFeatureInputError("fiscal_period must be Q1-Q4")
    if observation.metric_name not in INCOME_STATEMENT_METRICS:
        raise IncomeStatementFeatureInputError(
            f"unsupported income metric {observation.metric_name}"
        )
    if not observation.metric_value.is_finite():
        raise IncomeStatementFeatureInputError("metric_value must be finite")
    _require_aware(observation.available_at, "available_at")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise IncomeStatementFeatureInputError(f"{field_name} must be timezone-aware")
