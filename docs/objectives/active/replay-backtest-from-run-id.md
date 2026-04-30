# Replay Backtest From Run ID

Status:
Active

Objective:
Make a persisted Silver backtest reproducible from its durable run identity
without the operator re-entering strategy, universe, horizon, feature, or
policy parameters by hand.

Approval Mode:
chat-approved Symphony run

User Value:
Michael can trust a reported result because Silver can reload the exact
persisted run contract, rerun it, and prove whether the reproduced output still
matches the original claim.

Why Now:
Phase 1 proved the live momentum falsifier path on real data. The next useful
step is not more data or more features; it is making the proof replayable from
the metadata Silver already stores in `model_runs` and `backtest_runs`. Without
that, later AI text features can produce attractive reports that are hard to
audit.

Done When:
- A clear replay contract documents what can and cannot be reconstructed from a
  `model_run_id` and/or `backtest_run_id`.
- Runtime code can load a persisted falsifier run identity and reconstruct the
  normalized strategy, universe, horizon, target kind, cost assumptions,
  policy versions, feature set hash, and input fingerprints.
- A CLI path can rerun or dry-run replay from a persisted run id without the
  operator manually passing the original strategy, horizon, and universe.
- Replay validation compares reproduced identity and report-critical metrics
  against the stored run metadata and fails loudly on mismatch.
- Tests prove identical inputs replay deterministically and prove at least one
  changed replay input is detected as a mismatch.
- Existing Phase 1 commands and reports keep working.

Out Of Scope:
- New feature families beyond those already needed for the momentum falsifier.
- AI text features, prompt/model extraction, transcript ingest, SEC ingest, or
  news ingest.
- Portfolio construction, paper trading, or live capital execution.
- Destructive migrations or rewriting already-applied migrations.
- Claims that require new paid or live vendor calls during test validation.

Guardrails:
- No feature value may be used without an `available_at` rule.
- No backtest result may be accepted without costs, baselines, label-scramble,
  and reproducibility metadata.
- Replay must not silently substitute currently active available-at policies
  for the policies recorded on the original run.
- Replay must not rely on invocation-only fields such as timestamps, UUIDs,
  report paths, process ids, host/user names, or database surrogate ids for
  deterministic identity.
- Do not commit `.env`, API keys, local credentials, vendor data, generated
  reports, or local database state.

Project Adapter:
Use the Silver Phase 2 reproducibility contract:
- `SPEC.md`
- `docs/ARCHITECTURE.md`
- `docs/TESTING.md`
- `docs/PIT_DISCIPLINE.md`
- `db/migrations/004_backtest_metadata.sql`
- `db/migrations/005_backtest_metadata_replay_constraints.sql`

Runner Adapter:
Linear/Symphony

Expected Tickets:

- Define the replay contract
  Ticket Role: contract
  Dependency Group: backtest-replay
  Contracts Touched:
  - backtest-run-replay
  - model-run-identity
  Risk Class: semantic
  Purpose: Make the replay behavior precise before implementation changes
  backtest semantics.
  Expected Impact On Objective: Gives the implementation ticket a narrow
  contract for what run identity must reconstruct and what mismatches must
  reject.
  Technical Summary: Update the smallest relevant docs and tests to define
  replay inputs, deterministic identity fields, mismatch behavior, and
  non-goals.
  Owns:
  - `docs/TESTING.md`
  - `docs/ARCHITECTURE.md`
  - `SPEC.md`
  - focused contract tests
  Do Not Touch:
  - `db/migrations/004_backtest_metadata.sql`
  - `db/migrations/005_backtest_metadata_replay_constraints.sql`
  - live ingest or vendor clients
  Dependencies:
  - none
  Conflict Zones:
  - backtest reproducibility contract
  - docs describing model_runs and backtest_runs
  Validation:
  - `git diff --check`
  - `python -m pytest`
  - `ruff check .`
  Proof Packet:
  - replay contract summary
  - changed docs/tests
  - validation output

- Implement replay loading and identity checks
  Ticket Role: implementation
  Dependency Group: backtest-replay
  Contracts Touched:
  - backtest-run-replay
  - model-run-identity
  Risk Class: semantic
  Purpose: Teach Silver to reload persisted model/backtest run metadata and
  compare replayed identity against the stored claim.
  Expected Impact On Objective: Converts persisted metadata into an executable
  replay input instead of passive audit text.
  Technical Summary: Add repository/query helpers and runtime logic that load
  `model_runs` and `backtest_runs` by id/key, normalize replay inputs, and
  compare deterministic keys, policy versions, cost assumptions, feature set
  hash, input fingerprints, and report-critical metrics.
  Owns:
  - `src/silver/analytics/repository.py`
  - `scripts/run_falsifier.py`
  - `src/silver/reports/falsifier.py`
  - focused replay tests
  Do Not Touch:
  - unrelated ingest clients
  - new feature families
  - portfolio or paper trading modules
  Dependencies:
  - Define the replay contract
  Conflict Zones:
  - falsifier run identity
  - analytics run repository
  - report reproducibility metadata
  Validation:
  - `git diff --check`
  - `python -m pytest`
  - `ruff check .`
  - `python scripts/run_falsifier.py --check`
  Proof Packet:
  - replay helper/API summary
  - mismatch detection evidence
  - validation output

- Add the replay CLI path
  Ticket Role: integration
  Dependency Group: backtest-replay
  Contracts Touched:
  - backtest-run-replay
  Risk Class: semantic
  Purpose: Give operators one command to replay a persisted falsifier run from
  its run identity.
  Expected Impact On Objective: Makes replay usable by Michael and by later
  automated validation jobs.
  Technical Summary: Add a documented CLI mode, such as replay by
  `model_run_id` and/or `backtest_run_id`, that loads persisted metadata,
  reruns or dry-runs the falsifier replay, and prints explicit match/mismatch
  evidence.
  Owns:
  - `scripts/run_falsifier.py`
  - `docs/PHASE1_RUNBOOK.md`
  - `docs/TESTING.md`
  - `tests/test_run_falsifier_cli.py`
  Do Not Touch:
  - vendor ingest behavior
  - schema migrations unless explicitly proven necessary and non-destructive
  Dependencies:
  - Implement replay loading and identity checks
  Conflict Zones:
  - falsifier CLI
  - runbook commands
  Validation:
  - `git diff --check`
  - `python -m pytest`
  - `ruff check .`
  - `python scripts/run_falsifier.py --check`
  Proof Packet:
  - exact replay command
  - example dry-run/check output
  - validation output

- Validate replay determinism
  Ticket Role: validation
  Dependency Group: backtest-replay
  Contracts Touched:
  - backtest-run-replay
  - model-run-identity
  Risk Class: semantic
  Purpose: Prove the replay path catches drift instead of only providing a
  convenience wrapper.
  Expected Impact On Objective: Gives Michael evidence that replay can be used
  as a trust gate for future AI-text backtests.
  Technical Summary: Add or extend tests that replay an identical persisted
  run contract, assert identity/metric match, mutate one stable replay input,
  and assert mismatch failure. Update docs with the exact validation evidence.
  Owns:
  - `tests/`
  - `docs/TESTING.md`
  - `docs/objectives/active/replay-backtest-from-run-id.md`
  Do Not Touch:
  - live vendor data
  - unrelated orchestration scripts
  Dependencies:
  - Add the replay CLI path
  Conflict Zones:
  - replay tests
  - validation docs
  Validation:
  - `git diff --check`
  - `python -m pytest`
  - `ruff check .`
  - `python scripts/run_falsifier.py --check`
  Proof Packet:
  - deterministic replay test name(s)
  - mismatch test name(s)
  - remaining gaps or skipped live checks

  Direct Build Evidence (2026-04-30):
  - Added `test_backtest_replay_comparison_matches_identical_contract`.
  - Added `test_backtest_replay_comparison_ignores_surrogate_ids_for_same_stable_identity`.
  - Existing drift guard: `test_backtest_replay_comparison_names_drifted_identity_field`.
  - Existing CLI match guard: `test_replay_run_uses_stored_plan_and_prints_match_evidence`.
  - Existing CLI mismatch guard: `test_replay_run_fails_with_mismatch_evidence`.
  - RED/PROVE: temporarily restored `backtest_runs.model_run_id` comparison;
    the surrogate-id replay test failed on that exact field, then passed after
    the fix was restored.
  - Validation: `git diff --check`; `python -m pytest
    tests/test_backtest_metadata_repository.py tests/test_run_falsifier_cli.py
    -q`; `python -m pytest`; `ruff check .`; `python
    scripts/run_falsifier.py --check`.

Validation:
- `git diff --check`
- `python -m pytest`
- `ruff check .`
- `python scripts/run_falsifier.py --check`
- dry-run replay command once implemented

Conflict Zones:
- `scripts/run_falsifier.py`
- `src/silver/analytics/repository.py`
- `src/silver/reports/falsifier.py`
- `docs/TESTING.md`
- `docs/ARCHITECTURE.md`
- backtest reproducibility semantics
