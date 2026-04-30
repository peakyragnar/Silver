# Phase 1 Foundation Plan

Status: Completed

Goal: Silver can persist point-in-time data and reproduce a simple 12-1 momentum
signal on the seed universe.

Completion:
- Live Phase 1 falsifier proof landed in PR #85:
  `https://github.com/SilverEnv/Silver/pull/85`
- Merge commit: `5e2cc560122eec152cdd022ec5707d33dfaa9f69`
- Report path: `reports/falsifier/week_1_momentum.md`
- Result: `succeeded`, `model_run_id=2`, `backtest_run_id=2`
- Process note: the final live proof was completed by a local Codex run, not by
  Symphony workers.

## Scope

- Bootstrap Python project structure
- Add foundation migrations
- Seed trading calendar and initial securities
- Implement raw vault and FMP price ingest
- Compute forward labels
- Implement the first walk-forward harness
- Emit the first falsifier report

## Acceptance Criteria

- [x] `.env` is ignored and `.env.example` documents required variables
- [x] `pyproject.toml` defines the local Python package and test tooling
- [x] `db/migrations/001_foundation.sql` creates core schema objects
- [x] Trading calendar is seeded for 2014-2026
- [x] Seed universe contains 45 liquid equities across major non-REIT sectors
- [x] Prices can be ingested for the seed universe
- [x] Phase 1 analytics migration defines normalized prices, forward labels,
  versioned numeric features, and minimal analytics run metadata
- [x] Labels are computed for 5, 21, 63, 126, and 252 trading-day horizons
- [x] Momentum 12-1 feature is computed without lookahead
- [x] Backtest includes costs, baselines, regimes, and label-scramble
- [x] Falsifier report command writes to
  `reports/falsifier/week_1_momentum.md`
- [x] Phase 1 pipeline command runs bootstrap, price ingest, labels, momentum
  features, and report generation in deterministic order

## Validation

- [x] `git diff --check`
- [x] `python scripts/run_phase1_pipeline.py --check`
- [x] `python scripts/seed_reference_data.py --check`
- [x] `python scripts/seed_trading_calendar.py --check`
- [x] `python scripts/apply_migrations.py --check`
- [x] `python scripts/materialize_forward_labels.py --check`
- [x] `python -m pytest`
- [x] `ruff check .`
- [x] `python scripts/run_falsifier.py --check`
- [x] `python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed` (command exists; live run requires persisted feature and label prerequisites)

## Current Readiness

As of 2026-04-30, the non-mutating Phase 1 wiring checks pass:

- `python scripts/check_phase1_environment.py --check`
- `python scripts/run_phase1_pipeline.py --check`
- `python scripts/check_falsifier_inputs.py --check`

Live Phase 1 execution was proven with a reachable `DATABASE_URL`. Use:

```bash
python scripts/check_phase1_environment.py --check --live-db
```

before bootstrap, ingest, materialization, or falsifier apply-mode commands.
A set-but-invalid database URL is not sufficient readiness.

## Falsifier Command

Validate the full Phase 1 pipeline wiring without live database writes or FMP
calls:

```bash
python scripts/run_phase1_pipeline.py --check
```

Run the full Phase 1 pipeline after `DATABASE_URL` and `FMP_API_KEY` are set and
`psql` is on `PATH`:

```bash
python scripts/run_phase1_pipeline.py --universe falsifier_seed --horizon 63
```

Validate CLI/config/report-path wiring without live data:

```bash
python scripts/run_falsifier.py --check
```

Run the Week 1 momentum report after prices, labels, and momentum feature values
are materialized:

```bash
python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed
```

## Seed Universe Intent

The `falsifier_seed` universe is a 45-security liquid cross-sector falsifier
set. Membership is point-in-time in `config/seed_reference_data.yaml` and starts
on `2014-04-03` for this seed interval. It spans information technology,
communication services, consumer discretionary, consumer staples, health care,
financials, industrials, energy, materials, and utilities while excluding ETFs,
REITs, options, futures, crypto, and foreign ordinary shares.

Financials such as JPM, BAC, WFC, GS, V, and MA are included only to make the
falsifier exercise cross-sector equity plumbing. Broad securities schema support
does not mean early feature parity for banks or REITs. Bank-specific
fundamental features, REIT-specific features, and any generic assumption that
treats financials and REITs like industrial/software businesses remain out of
scope; no REIT is in the seed universe.

## Suggested Ticket Breakdown

1. Repo bootstrap and tooling
2. Database foundation migration and seed config
3. Calendar generation and seed securities
4. FMP client plus raw vault writer
5. Daily prices ingest
6. Forward labels
7. Momentum feature and walk-forward harness
8. Costs, regimes, label-scramble, and report

## Notes

Keep Phase 1 narrow. Text features, hypothesis generation, paper trading, and
portfolio execution are intentionally deferred until the falsifier harness is
honest.

2026-04-30 review follow-up: close two lineage gaps before further Phase 1
claims: failed FMP price ingests must not publish usable normalized rows, and
falsifier available-at policy metadata must be derived from the joined input
rows rather than globally active policies.

ARR-24 added the first in-memory walk-forward `momentum_12_1` runner with
point-in-time label availability checks, transaction-cost assumptions, and a
numeric momentum-rank baseline. The final falsifier report command,
label-scramble test, and full regime breakdown remain separate follow-up work.

2026-04-30 live proof update: the Phase 1 live path has produced
`reports/falsifier/week_1_momentum.md` from persisted FMP prices, forward
labels, and `momentum_12_1` feature values. The report status is `succeeded`
with `model_run_id=2` and `backtest_run_id=2`. The run also exposed and fixed
live-only contract gaps around the current FMP stable endpoint, nullable
Postgres date parameters, future-available price rows, feature materialization
cycle time, and the `falsifier_report_invocation` analytics run kind.
