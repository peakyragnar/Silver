# Earnings Release Events V0

This layer adds the market-knowledge clock for headline fundamentals.

## Purpose

FMP normalized statements tell us the reported values. Filing timestamps tell us
when the full 10-Q or 10-K was accepted. But income-statement headline numbers
often reach the market first through an earnings press release filed on Form 8-K
under Item 2.02.

This layer stores that release event separately so feature generation can choose
the right clock:

```text
release_available_at: earnings release / 8-K clock
filing_available_at: formal 10-Q / 10-K clock
release_timing: bmo / rth / amc / non_trading_day
```

## V0 Source

V0 uses SEC EDGAR:

```text
SEC submissions index
-> filter Form 8-K / 8-K/A with Item 2.02
-> fetch only candidate archive index
-> select likely EX-99.1 earnings exhibit
-> parse fiscal period identity from release text
-> classify the accepted timestamp against the NYSE session
```

This avoids treating every 8-K as an earnings event.

## Connection To Fundamentals

Release events connect to normalized fundamentals by period identity:

```text
security_id
fiscal_year
fiscal_period
period_end_date
```

The helper view is:

```text
silver.earnings_release_fundamental_values
```

It currently links release events to income-statement fundamental values only.
Cash-flow values should remain on the filing clock unless release text proves
they were disclosed.

## Entrypoint

Small Apple proof:

```bash
uv run python scripts/ingest_sec_earnings_releases.py \
  --ticker AAPL \
  --candidate-limit 1 \
  --sleep-seconds 0
```

Live mode requires:

```text
DATABASE_URL
SEC_USER_AGENT
migrations through 011_add_earnings_release_timing.sql
seeded sec_8k_material available_at policy
```

Run kind:

```text
sec_earnings_release_ingest
```

## Timing Buckets

The SEC accepted timestamp is preserved exactly. Silver also classifies the
timestamp against the NYSE session:

```text
bmo: before market open
rth: regular trading hours
amc: after market close
non_trading_day: weekend or holiday
```

The Apple proof produced an `amc` event:

```text
accepted_at: 2026-04-30 16:30:41 ET
release_available_at: accepted_at + 30 minutes
```
