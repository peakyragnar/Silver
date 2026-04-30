# Live Phase 1 Falsifier Proof

Status:
Active

Objective:
Prove the Phase 1 Silver pipeline works end to end on real persisted data.

Approval Mode:
chat-approved local run

User Value:
Michael gets a trustworthy base for continuing the build instead of stacking
Phase 2, models, or AI text work on unproven plumbing.

Why Now:
Offline validation is green, and the current review fixes address partial price
ingest leakage plus policy provenance. The remaining uncertainty is whether the
live database and FMP path can produce a complete reproducible momentum
falsifier report.

Decision Anchor:
- Goal: prove the Phase 1 pipeline from live inputs to falsifier report.
- Constraints: preserve point-in-time discipline, frozen run provenance, and
  visible failure handling.
- Evidence: local tests and check commands pass; live DB/FMP execution still
  needs proof.
- Falsifier: if live ingest or materialization cannot produce the momentum
  falsifier report, Phase 1 is not complete and later phases should wait.

Done When:
- Current review fixes are committed or otherwise explicitly accepted.
- `python scripts/check_phase1_environment.py --check --live-db` passes.
- Database bootstrap succeeds.
- FMP price ingest for `falsifier_seed` succeeds, or any vendor failures are
  raw-vaulted and visible.
- Forward labels are materialized for `5/21/63/126/252`.
- `momentum_12_1` is materialized from point-in-time safe inputs.
- `python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed`
  writes a reproducible report.
- The report includes costs, baselines, regimes, label-scramble, and
  reproducibility metadata.
- Handoff records commands run, key outputs, report path, and skipped checks.

Current Evidence:
- Live DB preflight passes when `.env` is sourced.
- Database bootstrap now completes through migrations, available-at policy seed,
  reference seed, and trading-calendar seed.
- FMP ingest dry-run resolves 45 `falsifier_seed` tickers for
  `2014-04-03..2026-04-30`.
- Initial live FMP attempts failed on HTTP 401 and then HTTP 403 legacy-endpoint
  rejection; both responses were raw-vaulted and their price-normalization runs
  were marked failed.
- Current live FMP ingest uses the stable dividend-adjusted endpoint and
  succeeded for 45 tickers, capturing 45 raw responses and writing 136,665
  `prices_daily` rows.
- Forward label materialization wrote 662,085 rows and skipped 21,240 expected
  unavailable or insufficient-history labels.
- `momentum_12_1` materialization wrote 125,280 feature values.
- The falsifier wrote `reports/falsifier/week_1_momentum.md` with status
  `succeeded`, `model_run_id=2`, and `backtest_run_id=2`.

Current Blocker:
- None for the live Phase 1 proof. Final local validation and review/merge
  handoff remain.

Out Of Scope:
- Phase 2 backtest replay infrastructure beyond what is needed to complete this
  live proof.
- New numeric feature families beyond `momentum_12_1`.
- AI text features, prompt/model extraction, and text artifact pipelines.
- Web UI, daily predictions, paper portfolio automation, and hypothesis-machine
  automation.
- Any live-capital trading path.

Guardrails:
- No feature value may be used without an `available_at` rule.
- No prediction or report may be written without frozen feature, model, prompt,
  and run metadata where applicable.
- No partial failed ingest rows may silently feed features or labels.
- Do not commit `.env`, API keys, local credentials, or vendor secrets.
- Keep changes small, reversible, and auditable.
- Do not delete existing processes, safeguards, tables, or data-retention paths
  without explicit approval.

Project Adapter:
Use the Silver Phase 1 runbook and point-in-time discipline:
- `docs/PHASE1_RUNBOOK.md`
- `docs/PIT_DISCIPLINE.md`
- `docs/TESTING.md`
- `docs/ARCHITECTURE.md`
- `SPEC.md`

Runner Adapter:
Local Codex run unless mirrored into Linear/Symphony by explicit follow-up.

Expected Tickets:

- Land partial-ingest and policy-provenance fixes
  Ticket Role: implementation
  Dependency Group: phase-1-foundation
  Contracts Touched:
  - prices_daily normalization provenance
  - feature and label point-in-time loading
  - falsifier policy provenance report
  Risk Class: semantic
  Purpose: Make the current review fixes the accepted foundation for the live
  proof.
  Expected Impact On Objective: Prevent failed partial ingests and unrelated
  active policy changes from contaminating falsifier evidence.
  Technical Summary: Commit or explicitly accept the repository changes that
  filter to succeeded normalization runs, relink identical price retries, and
  load policy versions from joined feature and label rows.
  Owns:
  - `src/silver/prices/repository.py`
  - `src/silver/features/repository.py`
  - `src/silver/labels/repository.py`
  - `scripts/run_falsifier.py`
  - related tests
  Do Not Touch:
  - unrelated schema or feature families
  Dependencies:
  - none
  Conflict Zones:
  - Phase 1 repository modules
  - falsifier CLI policy provenance logic
  Validation:
  - `python -m pytest`
  - `ruff check .`
  - `git diff --check`
  Proof Packet:
  - command output summary
  - changed files summary

- Prove live Phase 1 environment
  Ticket Role: validation
  Dependency Group: phase-1-live-proof
  Contracts Touched:
  - environment readiness
  - database connectivity
  Risk Class: safety
  Purpose: Confirm the local machine can safely run the live Phase 1 pipeline.
  Expected Impact On Objective: Removes environment uncertainty before vendor
  calls or data writes.
  Technical Summary: Run the non-live and live DB preflight checks using local
  `DATABASE_URL`, `FMP_API_KEY`, and `psql` availability.
  Owns:
  - environment validation evidence only
  Do Not Touch:
  - secrets files
  Dependencies:
  - Land partial-ingest and policy-provenance fixes
  Conflict Zones:
  - local database state
  Validation:
  - `python scripts/check_phase1_environment.py --check`
  - `python scripts/check_phase1_environment.py --check --live-db`
  Proof Packet:
  - preflight output summary
  - any missing dependency notes

- Execute live Phase 1 pipeline
  Ticket Role: integration
  Dependency Group: phase-1-live-proof
  Contracts Touched:
  - raw vendor ingestion
  - normalized prices
  - forward labels
  - momentum feature values
  - falsifier analytics run
  Risk Class: semantic
  Purpose: Produce the first live persisted end-to-end Phase 1 falsifier run.
  Expected Impact On Objective: Converts offline readiness into real data proof.
  Technical Summary: Bootstrap the database, ingest FMP prices for
  `falsifier_seed`, materialize labels and `momentum_12_1`, then run the
  falsifier for horizon 63.
  Owns:
  - generated database rows
  - generated reports under `reports/`
  Do Not Touch:
  - schema migrations except already approved migrations
  - new feature families
  Dependencies:
  - Prove live Phase 1 environment
  Conflict Zones:
  - local database state
  - vendor API quota
  - generated reports
  Validation:
  - `python scripts/bootstrap_database.py`
  - `python scripts/ingest_fmp_prices.py --universe falsifier_seed`
  - `python scripts/materialize_forward_labels.py --universe falsifier_seed --horizons 5,21,63,126,252`
  - `python scripts/materialize_momentum_12_1.py --universe falsifier_seed`
  - `python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed`
  Proof Packet:
  - command output summary
  - generated report path
  - analytics run identifiers
  - any raw-vaulted vendor failures

Validation:
- `git diff --check`
- `python -m pytest`
- `ruff check .`
- `python scripts/check_phase1_environment.py --check`
- `python scripts/check_phase1_environment.py --check --live-db`
- `python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed`

Conflict Zones:
- `docs/exec-plans/active/phase-1-foundation.md`
- `docs/objectives/active/`
- `src/silver/prices/repository.py`
- `src/silver/features/repository.py`
- `src/silver/labels/repository.py`
- `scripts/run_falsifier.py`
- local Postgres database state
- FMP vendor quota and availability
