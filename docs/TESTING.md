# Testing And Validation

Silver values falsification over optimistic backtests. Tests should make false
confidence hard.

## Validation Ladder

Run the narrowest meaningful check while iterating, then broaden before handoff.

```bash
git diff --check
python -m pytest
ruff check .
python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed
```

Not all commands exist at repository bootstrap. If a command does not exist yet,
record that it was unavailable.

## CI Merge-Readiness Gate

GitHub Actions runs the merge-readiness gate on every pull request and every
push to `main`. This gate is the shared pre-review proof that the repository
installs from declared dependencies, validates offline seed/config checks, runs
tests, and passes lint. It does not implement branch protection, auto-merge, or
a merge queue; those are future harness layers.

The local equivalent is:

```bash
uv sync --locked --group dev --python 3.10
source .venv/bin/activate
git diff --check
python scripts/bootstrap_database.py --check
python scripts/apply_migrations.py --check
python scripts/seed_available_at_policies.py --check
python scripts/seed_reference_data.py --check
python scripts/seed_trading_calendar.py --check
python -m pytest
ruff check .
```

The `--check` seed and migration commands are intentionally offline. They must
not require `DATABASE_URL`, `FMP_API_KEY`, or other live service credentials.

## Required Test Classes

- Unit tests for calendar math, `available_at` policy logic, costs, and labels
- Integration tests for raw ingest through normalized rows
- Backtest tests for walk-forward splits, label-scramble, costs, and baselines
- Reproducibility tests proving repeated runs produce identical outputs

## Phase 1 Gate

Phase 1 is complete only when a repeatable command reproduces 12-1 momentum on
the seed universe with realistic costs and emits a report.

Target command:

```bash
python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed
```

The integration test should assert positive net Sharpe for the tiny seed
universe and verify the run carries reproducibility metadata.

## Reporting

Backtest reports must include gross and net metrics, baseline comparison,
regime breakdown, label-scramble result, and the exact model/run metadata used
to reproduce the output.
