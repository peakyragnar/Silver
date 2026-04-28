from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from silver.sources.fmp.daily_prices import (
    FmpDailyPriceParseError,
    parse_historical_daily_prices,
)


FIXTURE_PATH = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "fmp"
    / "historical_price_full_aapl.json"
)


def test_parse_historical_daily_prices_returns_typed_rows_from_fixture() -> None:
    payload = _fixture_payload()

    rows = parse_historical_daily_prices(payload)

    assert [row.date.isoformat() for row in rows] == [
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
    ]
    assert {row.ticker for row in rows} == {"AAPL"}
    assert rows[0].open == Decimal("187.15")
    assert rows[0].high == Decimal("188.44")
    assert rows[0].low == Decimal("183.89")
    assert rows[0].close == Decimal("185.64")
    assert rows[0].adj_close == Decimal("184.68")
    assert rows[0].volume == 82488700
    assert rows[0].source == "fmp"
    assert rows[0].raw_metadata == {
        "change": -1.51,
        "changeOverTime": -0.0080684,
        "changePercent": -0.80684,
        "label": "January 02, 24",
        "source_date": "2024-01-02",
        "source_symbol": "AAPL",
        "unadjustedVolume": 82488700,
        "vwap": 185.99,
    }


def test_parse_historical_daily_prices_rejects_missing_required_fields() -> None:
    payload = _fixture_payload()
    del payload["historical"][0]["adjClose"]

    with pytest.raises(
        FmpDailyPriceParseError,
        match=r"historical\[0\]\.adjClose is required",
    ):
        parse_historical_daily_prices(payload)


@pytest.mark.parametrize(
    ("field", "value", "error"),
    (
        ("open", "182.15", r"historical\[0\]\.open must be a number"),
        ("close", 0, r"historical\[0\]\.close must be positive"),
        ("volume", -1, r"historical\[0\]\.volume must be non-negative"),
        ("volume", 1.5, r"historical\[0\]\.volume must be an integer"),
    ),
)
def test_parse_historical_daily_prices_rejects_bad_numeric_values(
    field: str,
    value: object,
    error: str,
) -> None:
    payload = _fixture_payload()
    payload["historical"][0][field] = value

    with pytest.raises(FmpDailyPriceParseError, match=error):
        parse_historical_daily_prices(payload)


def test_parse_historical_daily_prices_rejects_duplicate_dates() -> None:
    payload = _fixture_payload()
    payload["historical"][1]["date"] = payload["historical"][0]["date"]

    with pytest.raises(
        FmpDailyPriceParseError,
        match=r"duplicate date for AAPL: 2024-01-04",
    ):
        parse_historical_daily_prices(payload)


def test_parse_historical_daily_prices_orders_rows_deterministically() -> None:
    payload = _fixture_payload()

    first = parse_historical_daily_prices(payload)
    second = parse_historical_daily_prices(
        {"symbol": "AAPL", "historical": list(reversed(payload["historical"]))}
    )

    assert first == second
    assert [row.date.isoformat() for row in first] == sorted(
        row["date"] for row in payload["historical"]
    )


def _fixture_payload() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
