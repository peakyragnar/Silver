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
- Backtest replay from run identity is a read of that same registry boundary.
  A complete accepted-claim replay starts from `backtest_run_id` or
  `backtest_run_key`, loads the joined `model_runs` row, and reconstructs the
  normalized replay inputs from stored metadata only. `model_run_id` by itself
  is enough to audit model/prediction identity, but not enough to select one
  complete backtest claim when several backtests share a model run.
- Replay callers must fail closed on missing rows, broken joins, empty replay
  inputs, non-terminal accepted-claim rows, non-`succeeded` accepted-claim rows,
  or mismatches in code SHA, feature-set hash, feature snapshot/input
  fingerprints, windows, horizon, target kind, random seed, cost assumptions,
  parameters, policy versions, universe, metrics, baselines, regimes,
  label-scramble evidence, or multiple-comparisons setting. They must not fill
  gaps from current CLI defaults, current policy config, live vendor clients,
  report paths, timestamps, UUIDs, process ids, host/user names, or database
  surrogate ids.

Phase 3 starts the hypothesis registry with `silver.hypotheses` and
`silver.hypothesis_evaluations`. These tables are the bridge between a human or
agent idea and replayable backtest evidence:

- `hypotheses.hypothesis_key` is the stable external identifier for a testable
  candidate, such as `momentum_12_1`.
- A hypothesis stores the thesis, signal name, expected mechanism, optional
  universe, horizon, target kind, status, and JSON metadata. It does not store
  price data, labels, predictions, or validation decisions.
- `hypothesis_evaluations` links one hypothesis to one durable
  `backtest_runs` row and the joined `model_runs` row. The backtest remains the
  source of truth for costs, baselines, policy versions, replay inputs, and
  metrics.
- Evaluation status is a summary of the backtest evidence for navigation:
  `running`, `rejected`, `promising`, `accepted`, or `failed`. It must never
  replace replay validation or statistical acceptance gates.
- Recording an evaluation moves the hypothesis lifecycle status to the latest
  non-failed evidence state. Reseeding a known hypothesis must preserve that
  lifecycle status rather than resetting it to `proposed`.
- A hypothesis can be proposed and linked manually before any autonomous
  proposal loop exists. Automation should only build on this registry after the
  manual path is observable and repeatable.

Feature Candidate Pack v1 is the first manual multi-hypothesis bridge. It
loads deterministic numeric candidate definitions from
`config/feature_candidates.yaml`, materializes the needed feature values, runs
one generic rank falsifier per candidate, and links the resulting durable
backtest rows back into the hypothesis registry.

- Candidate materialization remains in the feature-store layer. It may read
  normalized prices and point-in-time universe membership, but it must not read
  labels, backtest metrics, or hypothesis outcomes.
- The first configured feature families are adjusted-close returns, average
  dollar volume, and realized volatility. Price-return variants are separate
  feature definitions so different formation windows produce distinct feature
  hashes and replay identities.
- Candidate YAML owns hypothesis identity, prose, source feature, selection
  direction, and materializer name. Python owns the allow-listed materializer
  implementations and validates that each `source_feature` matches the selected
  materializer before any database write.
- The falsifier reads one persisted numeric feature at a time and joins it to
  already materialized labels. `selection_direction = high` ranks larger raw
  values higher. `selection_direction = low` keeps raw feature values unchanged
  in `silver.feature_values` and only flips the ranking score inside the
  falsifier input.
- Model-run and backtest-run deterministic identity distinguish non-default
  `selection_direction` values while preserving `high` as the implicit legacy
  default. High-volatility and low-volatility are different hypotheses even
  when they share the same raw feature definition.
- The candidate pack can upsert hypotheses and record evaluations, but the
  authoritative evidence remains the linked `backtest_runs` and `model_runs`
  rows plus replay validation.

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
