# Point-In-Time Discipline

Point-in-time discipline is the core safety property of Silver. If this breaks,
the research result is invalid.

## Canonical Timestamps

- `event_at`: when the underlying event happened
- `published_at`: when the source says the information became public
- `available_at`: earliest time Silver may use the datum in simulation
- `ingested_at`: when Silver fetched the datum
- `asof_date`: simulated prediction date

A backtest at `asof_date = D` may use only data with `available_at <= D`.

## Required Rules

- Every source adapter must assign an `available_at_policy_id`.
- Every fact, event, artifact, and price row must carry `available_at`.
- Every feature must be computable from data available on or before its
  `asof_date`.
- Labels are unavailable until their horizon has elapsed.
- Backtests must fail closed when `available_at` is missing.

## Policy Config

Initial source-specific policy definitions live in
`config/available_at_policies.yaml`.

Validate them without a live database:

```bash
python scripts/seed_available_at_policies.py --check
```

Seed or update `silver.available_at_policies` after migrations are applied:

```bash
python scripts/seed_available_at_policies.py
```

The seed command reads `DATABASE_URL` from the environment unless
`--database-url` is provided.

## Trading Calendar

Phase 1 US equity sessions are configured in `config/trading_calendar.yaml` and
seeded from `db/seed/trading_calendar.csv`. The seed file covers every calendar
date from 2014-01-01 through 2026-12-31; weekends and market holidays are
explicit `is_session = false` rows so missing dates fail closed instead of
becoming silent calendar-day approximations.

Generate or refresh the deterministic seed CSV:

```bash
python scripts/seed_trading_calendar.py --write-seed
```

Validate the config, generated rows, and checked-in seed CSV without a live
database:

```bash
python scripts/seed_trading_calendar.py --check
```

Seed or update `silver.trading_calendar` after migrations are applied:

```bash
python scripts/seed_trading_calendar.py
```

The calendar uses `pandas_market_calendars` with the `XNYS` calendar. Early
closes are represented with `is_early_close = true` and the UTC
`session_close` timestamp; intraday microstructure beyond the close timestamp
is out of scope for Phase 1.

## Test Expectations

Add tests that deliberately attempt to use future data and assert rejection.
For every feature family, include at least one fixture where a source exists in
the database but is not yet visible at the tested `asof_date`.

## Review Smells

- Joining on reporting period without checking `available_at`
- Using latest ticker membership for old dates
- Using revised fundamentals without supersession timing
- Computing labels in the same path that generates predictions
- Treating vendor backfill timestamps as historical availability
