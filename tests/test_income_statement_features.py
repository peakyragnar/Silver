from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from silver.features.income_statement import (
    DILUTED_SHARES_CHANGE_YOY_DEFINITION,
    GROSS_MARGIN_DEFINITION,
    NET_MARGIN_DEFINITION,
    REVENUE_GROWTH_YOY_DEFINITION,
    FundamentalMetricObservation,
    compute_quarterly_income_feature,
)


SECURITY_ID = 101


def test_revenue_growth_yoy_requires_current_quarter_to_be_visible() -> None:
    observations = (
        _metric(
            1,
            fiscal_year=2024,
            fiscal_period="Q1",
            period_end_date=date(2024, 3, 31),
            metric_name="revenue",
            metric_value="100.00",
            available_at=_dt("2024-05-01T20:00:00+00:00"),
        ),
        _metric(
            2,
            fiscal_year=2025,
            fiscal_period="Q1",
            period_end_date=date(2025, 3, 31),
            metric_name="revenue",
            metric_value="125.00",
            available_at=_dt("2025-05-01T20:00:00+00:00"),
        ),
    )

    before = compute_quarterly_income_feature(
        security_id=SECURITY_ID,
        asof=_dt("2025-05-01T19:00:00+00:00"),
        observations=observations,
        definition=REVENUE_GROWTH_YOY_DEFINITION,
    )
    after = compute_quarterly_income_feature(
        security_id=SECURITY_ID,
        asof=_dt("2025-05-01T21:00:00+00:00"),
        observations=observations,
        definition=REVENUE_GROWTH_YOY_DEFINITION,
    )

    assert before.status == "missing_prior_year_metric"
    assert before.value is None
    assert after.status == "ok"
    assert after.value == 0.25
    assert after.available_at == _dt("2025-05-01T20:00:00+00:00")
    assert after.window.current_fiscal_year == 2025
    assert after.window.current_fiscal_period == "Q1"
    assert after.window.prior_period_end_date == date(2024, 3, 31)


def test_gross_margin_uses_latest_visible_complete_quarter() -> None:
    observations = (
        _metric(
            1,
            fiscal_year=2025,
            fiscal_period="Q1",
            period_end_date=date(2025, 3, 31),
            metric_name="revenue",
            metric_value="200.00",
            available_at=_dt("2025-05-01T20:00:00+00:00"),
        ),
        _metric(
            2,
            fiscal_year=2025,
            fiscal_period="Q1",
            period_end_date=date(2025, 3, 31),
            metric_name="gross_profit",
            metric_value="80.00",
            available_at=_dt("2025-05-01T20:05:00+00:00"),
        ),
    )

    result = compute_quarterly_income_feature(
        security_id=SECURITY_ID,
        asof=_dt("2025-05-01T21:00:00+00:00"),
        observations=observations,
        definition=GROSS_MARGIN_DEFINITION,
    )

    assert result.status == "ok"
    assert result.value == 0.4
    assert result.available_at == _dt("2025-05-01T20:05:00+00:00")
    assert result.window.metric_names == ("gross_profit", "revenue")
    assert result.window.source_value_ids == (1, 2)


def test_net_margin_allows_negative_income() -> None:
    observations = (
        _metric(
            1,
            fiscal_year=2025,
            fiscal_period="Q1",
            period_end_date=date(2025, 3, 31),
            metric_name="revenue",
            metric_value="200.00",
            available_at=_dt("2025-05-01T20:00:00+00:00"),
        ),
        _metric(
            2,
            fiscal_year=2025,
            fiscal_period="Q1",
            period_end_date=date(2025, 3, 31),
            metric_name="net_income",
            metric_value="-20.00",
            available_at=_dt("2025-05-01T20:00:00+00:00"),
        ),
    )

    result = compute_quarterly_income_feature(
        security_id=SECURITY_ID,
        asof=_dt("2025-05-01T21:00:00+00:00"),
        observations=observations,
        definition=NET_MARGIN_DEFINITION,
    )

    assert result.status == "ok"
    assert result.value == -0.1


def test_diluted_shares_change_yoy_uses_same_fiscal_quarter() -> None:
    observations = (
        _metric(
            1,
            fiscal_year=2024,
            fiscal_period="Q2",
            period_end_date=date(2024, 6, 30),
            metric_name="diluted_weighted_average_shares",
            metric_value="100.00",
            available_at=_dt("2024-08-01T20:00:00+00:00"),
        ),
        _metric(
            2,
            fiscal_year=2025,
            fiscal_period="Q2",
            period_end_date=date(2025, 6, 30),
            metric_name="diluted_weighted_average_shares",
            metric_value="90.00",
            available_at=_dt("2025-08-01T20:00:00+00:00"),
        ),
    )

    result = compute_quarterly_income_feature(
        security_id=SECURITY_ID,
        asof=_dt("2025-08-01T21:00:00+00:00"),
        observations=observations,
        definition=DILUTED_SHARES_CHANGE_YOY_DEFINITION,
    )

    assert result.status == "ok"
    assert result.value == -0.1
    assert result.window.current_fiscal_period == "Q2"
    assert result.window.prior_period_end_date == date(2024, 6, 30)


def _metric(
    metric_id: int,
    *,
    fiscal_year: int,
    fiscal_period: str,
    period_end_date: date,
    metric_name: str,
    metric_value: str,
    available_at: datetime,
) -> FundamentalMetricObservation:
    return FundamentalMetricObservation(
        id=metric_id,
        security_id=SECURITY_ID,
        period_end_date=period_end_date,
        fiscal_year=fiscal_year,
        fiscal_period=fiscal_period,
        metric_name=metric_name,
        metric_value=Decimal(metric_value),
        available_at=available_at,
        available_at_policy_id=7,
    )


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)
