# Testing And Validation

Silver values falsification over optimistic backtests. Tests should make false
confidence hard.

## Validation Ladder

Run the narrowest meaningful check while iterating, then broaden before handoff.

```bash
git diff --check
python scripts/check_falsifier_inputs.py --check
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
python scripts/materialize_momentum_12_1.py --check
python scripts/check_falsifier_inputs.py --check
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

## Backtest Metadata Registry

Runtime wiring tests for `model_runs` must prove each persisted model run has a
stable key, code SHA, feature-set hash, frozen feature snapshot or input
fingerprint, training/test windows, horizon, target kind, random seed,
cost-assumption set, parameters, and available-at policy versions.

Runtime wiring tests for `backtest_runs` must prove each persisted backtest run
has a stable key, a `model_run_id`, universe, horizon, target kind,
cost-assumption set, baseline metrics, headline metrics, regime metrics,
label-scramble result, multiple-comparisons setting when applicable, and a final
status. `insufficient_data` is a valid terminal status but is not an accepted
backtest claim; tests should assert it sets `finished_at`, records deterministic
insufficiency metadata, and satisfies the shipped `label_scramble_pass`
constraint without reporting alpha.

## Reporting

Backtest reports must include gross and net metrics, baseline comparison,
regime breakdown, label-scramble result, and the exact model/run metadata used
to reproduce the output.

Falsifier report traceability validation must resolve the reported
`backtest_run_id` through the durable `backtest_runs.model_run_id` join to
`model_runs` before the report artifact is written. The validation checks the
reported code SHA, feature-set hash, horizon, universe, cost assumptions,
baseline metrics, headline metrics, available-at policy versions, and joined
input fingerprint against stored metadata. The replay path is covered by
`test_traceability_snapshot_resolves_backtest_run_to_model_run_metadata` and
`test_report_traceability_validation_fails_clearly_on_metadata_mismatch`.

Reproducibility proof for a reported backtest must include the reported
`backtest_run_key` or `backtest_run_id`, the joined `model_run_id`, and evidence
that the joined registry rows match the report's frozen code SHA, feature-set
hash, feature snapshot or input fingerprint, training/test windows, random seed,
cost assumptions, universe, horizon, target kind, baseline metrics, headline
metrics, regime metrics, label-scramble result, and available-at policy
versions. `insufficient_data` rows may be validated for deterministic handling,
but they are no-claim evidence and do not satisfy accepted-backtest proof.
