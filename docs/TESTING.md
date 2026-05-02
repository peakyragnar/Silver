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
python scripts/materialize_feature_candidates.py --check
python scripts/check_falsifier_inputs.py --check
python scripts/run_feature_candidate_pack.py --check
python scripts/run_feature_candidate_walk_forward.py --check
python scripts/research_results_report.py --check
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
Schema and repository tests must reject rows missing non-empty cost assumptions,
policy versions, or replay inputs.

Falsifier model-run identity tests must prove that `model_run_key` is
deterministic for identical code SHA, joined feature/label input fingerprint,
normalized run config, feature set, training/test window, random seed,
model-run cost/execution assumptions, and available-at policy versions. The
same tests must prove the key changes when any one of those stable inputs
changes, and does not change when only invocation metadata changes. Fresh
invocation fields such as UUIDs, timestamps, process ids, host/user names,
output paths, report paths, or database surrogate ids must not be part of the
`model_runs` or `backtest_runs` create payload for a deterministic key.

Runtime wiring tests for `backtest_runs` must prove each persisted backtest run
has a stable key, a `model_run_id`, universe, horizon, target kind,
cost-assumption set, baseline metrics, headline metrics, regime metrics,
label-scramble result, multiple-comparisons setting when applicable, and a final
status. Schema and repository tests must reject a `succeeded` accepted-claim row
that omits costs, headline metrics, regime metrics, baselines, or
label-scramble metrics. `insufficient_data` is a valid terminal status but is
not an accepted backtest claim; tests should assert it sets `finished_at`,
records deterministic insufficiency metadata, and satisfies the shipped
`label_scramble_pass` constraint without reporting alpha.

## Hypothesis Registry

Hypothesis registry tests must prove a candidate can be seeded, linked to a
durable `backtest_run_id`, and listed with its latest evaluation. The registry
may summarize whether evidence looks `promising`, `rejected`, `running`, or
`failed`, but tests must keep the replayable backtest row as the source of
truth. In particular:

- `succeeded` backtests with passing label-scramble evidence may be summarized
  as `promising`.
- `succeeded` backtests with failed label-scramble evidence must be summarized
  as `rejected` with a deterministic failure reason.
- `failed` and `insufficient_data` backtests must not become accepted evidence.
- Tests must reject unknown hypothesis keys and missing backtest rows rather
  than creating detached evaluation notes.
- CLI rendering tests should prove the operator can see the latest linked
  backtest identity without querying Postgres by hand.

## Feature Candidate Pack

Feature candidate pack tests must prove the pack has stable candidate keys,
candidate definitions load from YAML with source-feature validation, each
candidate materializes only point-in-time feature values, candidate metadata
records the selection direction, and the batch runner links each hypothesis to
a durable `backtest_run_id`.

The generic falsifier tests must cover both high-is-better and low-is-better
ranking. Low-direction candidates should keep raw feature values in
`feature_values` and invert only the ranking input used by the falsifier. The
deterministic model and backtest identity must distinguish non-default
`selection_direction` values while preserving legacy `high` as the implicit
default, so replaying a low-volatility candidate cannot silently become a
high-volatility candidate.

Feature-family tests should cover each new allow-listed materializer before it
is added to `config/feature_candidates.yaml`, including no-lookahead behavior at
the daily price availability boundary.

Harder walk-forward candidate tests must prove that split-level window metrics
are persisted, the pack-level consistency rollup rejects unstable candidates,
and label-scramble failure still prevents registry promotion even when the
walk-forward rollup passes.

## Reporting

Backtest reports must include gross and net metrics, baseline comparison,
regime breakdown, label-scramble result, and the exact model/run metadata used
to reproduce the output.

Falsifier report tests must assert that the rendered markdown includes the
required evidence contract from `SPEC.md`: status, no-alpha-claim language,
run config, data coverage, PIT universe membership, durable model/backtest
identity, git SHA, feature hashes, joined input fingerprint, available-at
policy versions, random seed, target kind, execution assumptions, model
windows, gross/net headline metrics, baseline comparison, costs, regime
breakdown, label-scramble evidence, multiple-comparisons setting, and the
traceability validation result. Regime evidence must include the named regime
date ranges and sample counts. Label-scramble evidence must identify the scored
row source, selection rule, seed, trial count, alpha, observed score, null
summary, p-value, and pass/fail result, or a deterministic insufficiency/failure
reason.

Falsifier report traceability validation must resolve the reported
`backtest_run_id` through the durable `backtest_runs.model_run_id` join to
`model_runs` before the report artifact is written. The validation checks the
reported code SHA, feature-set hash, model training/test windows, target kind,
random seed, execution assumptions, horizon, universe, cost assumptions,
baseline metrics, headline metrics, available-at policy versions, and joined
input fingerprint against stored metadata. The traceability snapshot must also
expose the stored training/test windows, parameters, regime metrics, and
label-scramble payloads needed to audit the run identity. The replay path is
covered by
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

Replay-from-run-id contract tests must cover the read side of that proof:

- resolving `backtest_run_id` or `backtest_run_key` to exactly one
  `backtest_runs` row and its joined `model_runs` metadata
- proving the traceability snapshot carries every replay input needed for a
  rerun: code SHA, feature-set hash, feature snapshot or input fingerprints,
  windows, horizon, target kind, random seed, cost assumptions, parameters,
  available-at policy versions, universe, metrics, baselines, regimes,
  label-scramble evidence, and multiple-comparisons setting
- proving deterministic model/backtest identity changes when stable replay
  inputs change and stays unchanged when only invocation metadata changes
- rejecting missing rows, broken joins, empty replay inputs, non-terminal
  accepted-claim rows, non-`succeeded` accepted-claim rows, and mismatches in
  any replay identity or report-critical metric field
- proving replay paths do not call live ingest or vendor clients and do not
  derive deterministic identity from UUIDs, timestamps, process ids, host/user
  names, output paths, report paths, or database surrogate ids

Deterministic replay comparison is covered by
`test_backtest_replay_comparison_matches_identical_contract`,
`test_backtest_replay_comparison_ignores_surrogate_ids_for_same_stable_identity`,
and `test_backtest_replay_comparison_names_drifted_identity_field`. End-to-end
CLI replay evidence is covered by
`test_replay_run_uses_stored_plan_and_prints_match_evidence` and
`test_replay_run_fails_with_mismatch_evidence`.

## Replay CLI Validation

The operator replay path starts from a durable backtest identity, not from
manually re-entered strategy, universe, horizon, feature, or policy arguments.
Use dry-run mode to validate the stored replay contract without writing a new
report:

```bash
python scripts/run_falsifier.py --replay-backtest-run-id 2 --replay-dry-run
```

Expected dry-run evidence includes:

```text
OK: falsifier replay dry-run loaded accepted claim metadata
Replay command: python scripts/run_falsifier.py --replay-backtest-run-id 2
Resolved run command: python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed
Evidence: stored accepted-claim metadata matched replay contract
Evidence: rerun not executed (--replay-dry-run)
```

Use replay mode without `--replay-dry-run` to rerun and compare the replayed
claim metadata against the stored accepted claim:

```bash
python scripts/run_falsifier.py --replay-backtest-run-id 2 --output-path reports/falsifier/week_1_momentum_replay.md
```

A match prints `Evidence: all replay-critical identity and metric fields
matched`. A mismatch must exit non-zero and name the drifted field. `model_run_id`
by itself remains audit metadata only; full replay requires `backtest_run_id` or
`backtest_run_key` so Silver can select exactly one complete backtest claim.
