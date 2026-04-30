# wire-backtest-metadata-registry

Objective:
Wire the durable backtest metadata registry so every accepted backtest result is
reproducible from its run identity.

User Value:
Michael can review a backtest claim and see the exact code, feature set,
training window, random seed, execution assumptions, and point-in-time policy
versions needed to reproduce or reject it.

Why Now:
Silver's product contract already makes reproducibility metadata a day-one law,
and Phase 2 starts the durable `model_runs` and `backtest_runs` registry. This
Objective coordinates the schema, runner, report, and validation work before it
is split into implementation tickets.

Done When:
- Backtest metadata contracts are durable in schema and docs.
- Model run records capture code SHA, feature set hash, training window,
  random seed, execution assumptions, and available-at policy version set.
- Backtest run records reference model runs and capture universe, horizon,
  costs, baselines, metrics, and reproducibility metadata.
- Falsifier or walk-forward reports expose the metadata required to reproduce
  the run.
- Validation proves a run can be traced from a reported result back to the
  frozen inputs that produced it.

Out Of Scope:
- No new text features.
- No portfolio or paper-trading execution.
- No vendor fetch expansion.
- No Linear, GitHub, migration, or steward automation.
- No Arrow code, schema imports, or analyst-facing views.

Guardrails:
- No feature value may be used without an `available_at` rule.
- No prediction may be written without frozen feature, model, and prompt
  versions.
- No backtest result may be reported without costs, baselines, and
  reproducibility metadata.
- Keep model, prompt, feature, and execution-assumption versions immutable once
  referenced by a run.
- Keep schema work isolated to one migration-owner ticket before dependent
  runner/report tickets proceed.
- Do not commit `.env`, API keys, vendor secrets, or local credentials.

Expected Tickets:
- Confirm the metadata registry contract
  Ticket Role: contract
  Dependency Group: backtest-metadata-registry
  Contracts Touched:
  - backtest-run-registry
  Risk Class: semantic
  Purpose: Align `SPEC.md`, architecture docs, and migration ownership before
  implementation starts.
  Expected Impact On Objective: Dependent tickets build against one accepted
  model/backtest run contract instead of inventing registry shape independently.
  Technical Summary: Review the existing metadata schema, document the durable
  fields, and identify whether a schema owner ticket is actually required.
  Owns:
  - `SPEC.md`
  - `docs/ARCHITECTURE.md`
  - `docs/TESTING.md`
  Do Not Touch:
  - `scripts/run_falsifier.py`
  - `src/silver/features/`
  Dependencies:
  - none
  Conflict Zones:
  - `SPEC.md`
  - `docs/ARCHITECTURE.md`
  - `docs/TESTING.md`
  Validation:
  - `git diff --check`
  Proof Packet:
  - accepted registry field list
  - explicit statement on whether a migration is required
- Add or complete durable metadata schema
  Ticket Role: contract
  Dependency Group: backtest-metadata-registry
  Contracts Touched:
  - backtest-run-registry
  Risk Class: migration
  Purpose: Create or adjust durable `model_runs` and `backtest_runs` metadata
  storage only if the contract ticket proves the existing schema is insufficient.
  Expected Impact On Objective: The Objective gets a stable database contract
  before runner and report tickets write reproducibility records.
  Technical Summary: Add the minimal migration or schema documentation needed
  for code SHA, feature set hash, training window, seed, assumptions, PIT policy
  versions, universe, horizon, costs, baselines, metrics, and run metadata.
  Owns:
  - `db/migrations/`
  - `tests/test_migrations.py`
  Do Not Touch:
  - `scripts/run_falsifier.py`
  - `src/silver/features/`
  Dependencies:
  - Confirm the metadata registry contract
  Conflict Zones:
  - `db/migrations/`
  - `tests/`
  Validation:
  - `python scripts/apply_migrations.py --check`
  - `python -m pytest tests/test_migrations.py`
  Proof Packet:
  - migration number or no-migration justification
  - schema validation output
- Wire runners to create `model_runs`
  Ticket Role: implementation
  Dependency Group: backtest-metadata-registry
  Contracts Touched:
  - backtest-run-registry
  Risk Class: semantic
  Purpose: Make walk-forward or falsifier runners create durable model run
  records before reporting results.
  Expected Impact On Objective: Each reported backtest can identify the frozen
  model/run input identity used to produce it.
  Technical Summary: Update runner code to compute deterministic model-run
  identity from code SHA, features, training window, seed, execution
  assumptions, and PIT policy versions.
  Owns:
  - `scripts/run_falsifier.py`
  - `src/silver/backtest/`
  - `src/silver/models/`
  Do Not Touch:
  - `db/migrations/`
  - `src/silver/features/`
  Dependencies:
  - Confirm the metadata registry contract
  - Add or complete durable metadata schema
  Conflict Zones:
  - `scripts/run_falsifier.py`
  - `src/silver/backtest/`
  - `src/silver/models/`
  Validation:
  - `python scripts/run_falsifier.py --check`
  - `python -m pytest`
  Proof Packet:
  - sample `model_run` identity
  - validation command output
- Wire result writing to create `backtest_runs`
  Ticket Role: implementation
  Dependency Group: backtest-metadata-registry
  Contracts Touched:
  - backtest-run-registry
  Risk Class: semantic
  Purpose: Persist `backtest_runs` records with costs, baselines, metrics, and
  reproducibility metadata.
  Expected Impact On Objective: The accepted backtest claim can be traced from
  report output to durable run metadata.
  Technical Summary: Attach backtest run writes to the existing result path and
  ensure reruns use deterministic keys for identical inputs.
  Owns:
  - `scripts/run_falsifier.py`
  - `src/silver/backtest/`
  - `tests/`
  Do Not Touch:
  - `db/migrations/`
  Dependencies:
  - Wire runners to create `model_runs`
  Conflict Zones:
  - `scripts/run_falsifier.py`
  - `src/silver/backtest/`
  - `tests/`
  Validation:
  - `python scripts/run_falsifier.py --check`
  - `python -m pytest`
  Proof Packet:
  - sample `backtest_run` identity
  - deterministic rerun evidence
- Surface reproducibility metadata in falsifier reports
  Ticket Role: integration
  Dependency Group: backtest-metadata-registry
  Contracts Touched:
  - backtest-run-registry
  - falsifier-report
  Risk Class: semantic
  Purpose: Show registry-backed reproducibility metadata in the user-facing
  falsifier report.
  Expected Impact On Objective: Michael can review a report and see the exact
  durable identity needed to replay or reject the result.
  Technical Summary: Update report rendering to include model/backtest run
  identity, frozen inputs, costs, baselines, and required falsifier evidence.
  Owns:
  - `src/silver/reports/falsifier.py`
  - `reports/falsifier/`
  - `tests/`
  Do Not Touch:
  - `db/migrations/`
  Dependencies:
  - Wire result writing to create `backtest_runs`
  Conflict Zones:
  - `src/silver/reports/`
  - `reports/falsifier/`
  Validation:
  - `python -m pytest tests/test_falsifier_report.py`
  - `python scripts/run_falsifier.py --check`
  Proof Packet:
  - rendered report path
  - metadata fields visible in report
- Add replay or traceability validation
  Ticket Role: validation
  Dependency Group: backtest-metadata-registry
  Contracts Touched:
  - backtest-run-registry
  Risk Class: semantic
  Purpose: Prove a reported result can be traced back to the same frozen
  metadata inputs.
  Expected Impact On Objective: The Objective is complete only when the
  reproducibility promise can be checked end to end.
  Technical Summary: Add focused tests or a check path that starts with report
  metadata and verifies the matching persisted model/backtest run identity.
  Owns:
  - `tests/`
  - `scripts/`
  Do Not Touch:
  - `db/migrations/`
  Dependencies:
  - Surface reproducibility metadata in falsifier reports
  Conflict Zones:
  - `tests/`
  - `scripts/run_falsifier.py`
  Validation:
  - `git diff --check`
  - `python -m pytest`
  - `ruff check .`
  - `python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed`
  Proof Packet:
  - traceability evidence from report to run registry

Validation:
- `git diff --check`
- `python -m pytest`
- `ruff check .`
- `python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed`
- Reproducibility evidence showing the same reported run can be traced to the
  same frozen metadata inputs.

Conflict Zones:
- `db/migrations/`
- `src/silver/backtest/`
- `src/silver/models/`
- `scripts/run_falsifier.py`
- `reports/falsifier/`
- `tests/`
- `SPEC.md`
- `docs/ARCHITECTURE.md`
- `docs/TESTING.md`
