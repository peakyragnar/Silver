# Architecture

Silver is a layered point-in-time research system. The target structure is in
`SPEC.md`; this file turns it into working rules for agents.

## Layer Order

Code should move data in this order:

1. ingest clients and raw vault
2. point-in-time normalization
3. feature store
4. labels
5. models and walk-forward backtests
6. hypotheses
7. portfolio simulation and paper trading
8. runtime jobs and reports

Avoid reverse dependencies. For example, ingestion code must not depend on
backtest code, and feature code must not read prediction outcomes.

## Expected Repository Shape

```text
config/
db/migrations/
db/seed/
src/silver/
scripts/
tests/
reports/
```

Create only the directories needed for the current phase. Do not scaffold large
empty systems just to mirror the final spec.

## Database Boundary

All application tables live in the `silver` Postgres schema. Migrations are the
source of truth for table shape. Python code should use typed helper functions
or repository-local query helpers rather than ad hoc SQL scattered across
scripts.

Numbered SQL migrations live under `db/migrations/`. Run
`python scripts/apply_migrations.py --check` to validate migration order and
static schema expectations without a live database.

Phase 2 starts the durable backtest reproducibility registry with
`silver.model_runs` and `silver.backtest_runs`. These tables hold run metadata
only until model and backtest runners are explicitly wired to write them.

Runtime writers must treat migration `004_backtest_metadata.sql` as the table
shape contract:

- `model_run_key` and `backtest_run_key` are stable, non-empty external keys
  for idempotent writes. `backtest_runs.model_run_id` is the durable join back
  to the exact model run and uses `ON DELETE RESTRICT`.
- `model_runs` maps the reproducibility contract through `code_git_sha`,
  `feature_set_hash`, `feature_snapshot_ref`, training/test date windows,
  `horizon_days`, `target_kind`, `random_seed`, `cost_assumptions`,
  `parameters`, `available_at_policy_versions`, and `input_fingerprints`.
- `backtest_runs` maps claim evidence through `universe_name`, `horizon_days`,
  `target_kind`, `cost_assumptions`, `parameters`, `metrics`,
  `metrics_by_regime`, `baseline_metrics`, `label_scramble_metrics`,
  `label_scramble_pass`, and `multiple_comparisons_correction`.
- Valid statuses are `running`, `succeeded`, `failed`, and
  `insufficient_data`. `running` rows have no `finished_at`; every terminal
  status sets `finished_at`. `succeeded` rows must have non-empty `metrics`.
- `insufficient_data` is a terminal no-claim status. Until a migration review
  changes the shipped constraint, runtime writers must set
  `label_scramble_pass = false` for insufficient-data rows and put deterministic
  insufficiency details in JSON metadata such as `parameters` or `metrics`.

Current contract gap: the shipped schema provides JSON object columns for
baselines, regime metrics, label-scramble metrics, and cost assumptions, but it
does not enforce non-empty baseline/regime/label-scramble payloads for a
`succeeded` backtest at the database level. Runtime repositories and tests must
enforce those accepted-claim requirements; stronger database enforcement needs a
follow-up Safety Review/migration decision instead of an in-place edit to
migration 004.

For a clean local Postgres database, prefer the single bootstrap command:

```bash
python scripts/bootstrap_database.py --check
DATABASE_URL=postgresql://... python scripts/bootstrap_database.py
```

The bootstrap order is deterministic:

1. `scripts/apply_migrations.py`
2. `scripts/seed_available_at_policies.py`
3. `scripts/seed_reference_data.py`
4. `scripts/seed_trading_calendar.py`

`--check` runs each step's no-database validation path. Apply mode requires
`DATABASE_URL` or `--database-url`, fails fast on the first failing step, and
uses the existing individual migration and seed scripts unchanged. The
migration apply path records checksums in `silver.schema_migrations` and
requires the `psql` client.

## External Data Boundary

Allowed sources for v1 are FMP, SEC EDGAR, optional Arrow raw byte caches, and
optional Norgate data later for delistings. Do not depend on Arrow normalized
tables, Arrow Python code, or analyst-facing views.

## LLM Boundary

LLMs may extract structured text features, propose hypotheses, and explain
validated results. They must not compute returns, write labels, write
predictions, score outcomes, override risk controls, or validate their own
ideas.
