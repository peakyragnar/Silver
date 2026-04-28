# Phase 1 Foundation Plan

Goal: Silver can persist point-in-time data and reproduce a simple 12-1 momentum
signal on the seed universe.

## Scope

- Bootstrap Python project structure
- Add foundation migrations
- Seed trading calendar and initial securities
- Implement raw vault and FMP price ingest
- Compute forward labels
- Implement the first walk-forward harness
- Emit the first falsifier report

## Acceptance Criteria

- [ ] `.env` is ignored and `.env.example` documents required variables
- [ ] `pyproject.toml` defines the local Python package and test tooling
- [ ] `db/migrations/001_foundation.sql` creates core schema objects
- [x] Trading calendar is seeded for 2014-2026
- [ ] Seed universe contains NVDA, MSFT, AAPL, GOOGL, and JPM
- [ ] Prices can be ingested for the seed universe
- [ ] Labels are computed for 5, 21, 63, 126, and 252 trading-day horizons
- [ ] Momentum 12-1 feature is computed without lookahead
- [ ] Backtest includes costs, baselines, regimes, and label-scramble
- [ ] Report is written to `reports/falsifier/week_1_momentum.md`

## Validation

- [x] `git diff --check`
- [x] `python scripts/seed_trading_calendar.py --check`
- [x] `python scripts/apply_migrations.py --check`
- [x] `python -m pytest`
- [x] `ruff check .`
- [ ] `python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed` (script not present yet)

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
