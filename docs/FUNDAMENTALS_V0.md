# Fundamentals V0

This document defines the first useful fundamentals layer for Silver. The goal
is to get normalized business data into the same feature and falsifier loop that
already works for prices, without taking on every SEC filing nuance at once.

## Decision

Use FMP normalized financial statements as the v0 modeling source. Use SEC data
as raw evidence, timing support, and later reconciliation.

```text
FMP normalized statements
-> raw vault
-> normalized fundamental values
-> fundamental features
-> falsifier test layer

SEC companyfacts / filings
-> raw vault
-> evidence, timing support, reconciliation
```

This is not an FMP-versus-SEC decision. It is a separation of jobs:

- FMP helps us move faster with normalized statement rows.
- SEC keeps the system auditable against primary filing evidence.
- Silver owns the formulas, feature definitions, point-in-time rules, and
  falsifier evidence.

## Scope

V0 should ingest only the normalized statement data needed for the first
fundamental features.

Include:

```text
source: FMP normalized financial statements
periods: annual and quarterly
fetch/normalize start: 2014 onward
universe: falsifier_seed first
```

The v0 price research window and normalized fundamental fetch window start in
2014, while the trading calendar starts in 2013 so point-in-time clocks can
handle fiscal-2014 filings and releases that were published before calendar
2014. Year-over-year features should begin only once a comparable prior-year
v0 metric exists. With 2014 as the first normalized fundamentals year, many YoY
fundamental features will naturally begin in 2015.

Do not ingest everything yet.

Defer:

```text
all SEC concept parsing
all FMP ratios as feature truth
all FMP growth endpoints as feature truth
all companies outside the approved universe
all statement fields not needed by the first features
transcripts
news
segments
guidance
insider data
13F data
```

FMP ratio and growth endpoints may be useful diagnostics later, but v0 features
should be computed by Silver from normalized statement values so the formula is
auditable and replayable.

## FMP Endpoints

Fetch only the statement families needed for the first metrics:

```text
income statement
cash flow statement
balance sheet statement only if a selected v0 metric needs it
```

Each live response must be written to `silver.raw_objects` before parsing or
normalization, including non-2xx responses returned by the transport.

If an FMP endpoint returns extra fields, raw-vault the exact response but only
normalize selected v0 metrics.

## Selected Metrics

Normalize the smallest useful metric set first:

```text
revenue
gross_profit
operating_income
net_income
operating_cash_flow
capital_expenditure
free_cash_flow
diluted_weighted_average_shares
```

`diluted_weighted_average_shares` is required for share-count features. Use the
diluted weighted average share count, such as FMP's
`weightedAverageShsOutDil` field when present.

Do not silently fall back from diluted shares to basic shares. If diluted shares
are missing, the diluted-share feature should fail closed or mark that
company-period insufficient until we make an explicit separate decision.

## First Features

The first candidate fundamental features are:

```text
revenue_growth_yoy
gross_margin
operating_margin
net_margin
diluted_shares_change_yoy
```

Formula intent:

```text
revenue_growth_yoy =
  revenue(current comparable period) / revenue(prior-year comparable period) - 1

gross_margin =
  gross_profit / revenue

operating_margin =
  operating_income / revenue

net_margin =
  net_income / revenue

diluted_shares_change_yoy =
  diluted_weighted_average_shares(current comparable period)
  / diluted_weighted_average_shares(prior-year comparable period) - 1
```

For quarterly rows, compare to the same fiscal quarter in the prior fiscal year.
If annual features are added later, compare to the prior fiscal year.

The first implemented feature pack uses quarterly income-statement rows only.
Each feature is materialized daily by carrying forward the latest quarterly
period whose `available_at` timestamp is visible at that trading day's snapshot
time. Cash-flow features remain deferred until we intentionally add them.

## Point-In-Time Rules

Fundamental rows are predictive data, so they must not be available before the
underlying filing was available to the market.

Preferred timing source:

```text
accepted_at from the filing/source row
```

If FMP supplies an accepted timestamp, use that as the basis for
`available_at`. If FMP only supplies a filing date and no accepted timestamp,
normalization should fail closed for predictive use until we either ingest SEC
submission metadata or define a conservative fallback rule.

Do not use `fetched_at` as the historical availability time for normalized
fundamentals. `fetched_at` proves when Silver downloaded the vendor response,
not when investors could have known the filing information.

The likely available-at rule for normalized filing-derived fundamentals is:

```text
filing accepted_at
-> next trading session at 09:30 ET
```

This should be tied to the existing filing/fundamental available-at policies,
not embedded as ad hoc script logic.

## Normalized Storage Shape

Prefer a narrow, metric-oriented table for v0 rather than separate statement
tables:

```text
silver.fundamental_values

security_id
period_end_date
fiscal_year
fiscal_period
period_type
statement_type
metric_name
metric_value
currency
source_system
source_field
raw_object_id
accepted_at
filing_date
available_at
available_at_policy_id
normalized_by_run_id
metadata
```

This keeps the first build small while still supporting new metrics later
without repeated schema changes.

## SEC Relationship

The SEC companyfacts raw ingest already gives Silver primary-source raw
evidence. It should not become the first modeling normalization path for v0.

Use SEC data next for:

```text
filing accepted timestamps
accession-level evidence
FMP-to-SEC reconciliation
restatement and revision checks
```

The first useful reconciliation question is:

```text
Did the FMP normalized value for a selected metric match the corresponding SEC
filing evidence for the same company and period?
```

Do not block v0 FMP normalized fundamentals on full SEC reconciliation. Add the
reconciliation path after selected FMP metrics are observable.

## Layer Boundary

The falsifier is not ingest.

Keep the layers separate:

```text
ingest:
  vendor response -> silver.raw_objects

normalize:
  raw response -> normalized fundamental values

features:
  normalized values -> silver.feature_values

test:
  silver.feature_values + silver.forward_return_labels -> falsifier
```

The first FMP fundamentals objective should stop at raw-vaulted vendor
responses plus normalized v0 fundamental values. Fundamental feature generation
and falsifier runs are follow-on objectives.

## Current Entrypoint

The v0 ingest entrypoint is:

```bash
uv run python scripts/ingest_fmp_fundamentals.py --dry-run --limit 3
```

Live mode requires `DATABASE_URL`, `FMP_API_KEY`, migrations through
`009_fmp_fundamental_values.sql`, seeded reference data, seeded trading
calendar rows, and seeded available-at policies. It writes run metadata as:

```text
run_kind: fmp_fundamentals_normalization
table: silver.fundamental_values
normalization_version: fmp_fundamentals_v0
```

## Acceptance Criteria

The first implementation should prove:

```text
check mode validates config without DB or FMP access
dry run resolves universe tickers and requested FMP endpoints
live mode raw-vaults FMP statement responses
normalization writes only selected v0 metrics
available_at is present and policy-backed for every normalized row
diluted shares are used for share-count features
missing diluted shares fail closed
tests cover annual and quarterly rows
tests cover missing accepted_at / unavailable PIT inputs
```
