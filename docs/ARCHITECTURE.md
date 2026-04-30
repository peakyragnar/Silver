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

Migration `004_backtest_metadata.sql` owns the Phase 2 registry table shape.
Migration `005_backtest_metadata_replay_constraints.sql` adds non-destructive
replay-completeness constraints. Future changes to `silver.model_runs` or
`silver.backtest_runs` must ship as a new numbered migration under
`db/migrations/`; applied migrations are not rewritten. A change that alters
accepted-claim meaning, point-in-time policy semantics, retention, or
destructive behavior needs a Safety Review or an explicit migration-owner
ticket before implementation.

Runtime writers must treat migrations `004_backtest_metadata.sql` and
`005_backtest_metadata_replay_constraints.sql` as the table shape contract:

- `model_run_key` and `backtest_run_key` are stable, non-empty external keys
  for idempotent writes. `backtest_runs.model_run_id` is the durable join back
  to the exact model run and uses `ON DELETE RESTRICT`.
- `model_runs` maps the reproducibility contract through `code_git_sha`,
  `feature_set_hash`, `feature_snapshot_ref`, training/test date windows,
  `horizon_days`, `target_kind`, `random_seed`, `cost_assumptions`,
  `parameters`, `available_at_policy_versions`, and `input_fingerprints`.
  Cost assumptions and policy versions must be non-empty, and each row must
  carry either a frozen feature snapshot reference or non-empty input
  fingerprints.
- `backtest_runs` maps claim evidence through `universe_name`, `horizon_days`,
  `target_kind`, `cost_assumptions`, `parameters`, `metrics`,
  `metrics_by_regime`, `baseline_metrics`, `label_scramble_metrics`,
  `label_scramble_pass`, and `multiple_comparisons_correction`.
- Valid statuses are `running`, `succeeded`, `failed`, and
  `insufficient_data`. `running` rows have no `finished_at`; every terminal
  status sets `finished_at`. `succeeded` rows must have non-empty costs,
  headline metrics, regime metrics, baseline metrics, and label-scramble
  metrics at both the repository and database boundaries.
- `insufficient_data` is a terminal no-claim status. Runtime writers must set
  `label_scramble_pass = false` for insufficient-data rows and put deterministic
  insufficiency details in JSON metadata such as `parameters` or `metrics`.
- Accepted claims are limited to terminal `succeeded` backtest rows whose
  report identity resolves through `backtest_runs.model_run_id` to the frozen
  model-run metadata. Reports, markdown files, and CLI arguments can display
  metadata, but they are not the authoritative registry when durable rows are
  available.

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

## FMP Response Audit Boundary

Every FMP HTTP response produced by the transport is raw-vault evidence,
including:

- successful 2xx responses
- transient failed attempts that will be retried
- terminal non-2xx responses that will raise an FMP client error

The source client must attempt the raw-vault write before JSON parsing,
normalization, retry sleep, success return, or raising an HTTP error. Transport
exceptions that produce no HTTP status and no response body are not raw objects;
they may be logged or surfaced as client errors with secrets redacted.

FMP raw-vault writes use the existing `silver.raw_objects` fields for redacted
request URL, redacted params, HTTP status, content type, exact response bytes,
body hash, request fingerprint, `fetched_at`, and JSON metadata. Attempt
metadata for this contract belongs in `raw_objects.metadata` and must identify:

- `audit_contract`: `fmp-response-audit-v1`
- one-based `attempt_number`
- `max_retries` and derived `max_attempts`
- whether the status is retryable
- whether this response is terminal for the request
- the attempt outcome: `success`, `retry_scheduled`, or `terminal_failure`

No schema migration is required to store distinct failed-response bytes,
terminal non-2xx bodies, transient-before-success bodies, redacted request
evidence, or per-row attempt metadata. The existing uniqueness key
`(vendor, endpoint, params_hash, raw_hash)` does mean byte-identical retries for
the same request can resolve to the same raw object. A raw-vault write must
still be attempted for each transport-produced response, but strict per-attempt
row cardinality for byte-identical retry bodies requires a follow-up schema
ticket, such as an additive attempt-event table keyed to `raw_objects.id`.

## LLM Boundary

LLMs may extract structured text features, propose hypotheses, and explain
validated results. They must not compute returns, write labels, write
predictions, score outcomes, override risk controls, or validate their own
ideas.
