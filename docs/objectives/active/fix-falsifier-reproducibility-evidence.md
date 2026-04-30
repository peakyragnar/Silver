# fix-falsifier-reproducibility-evidence

Objective:
Fix falsifier reproducibility and evidence integrity so every reported
falsifier result has deterministic run identity and reviewable adversarial
evidence.

User Value:
Michael can trust a falsifier report because the reported result is tied to a
deterministic run identity and includes the falsifier evidence needed to judge
whether the strategy survived basic adversarial checks.

Why Now:
The previous metadata registry work made run identity durable, but review found
three P1 gaps in the falsifier path: model run identity is not deterministic,
the markdown report omits required evidence, and label scramble is not testing
the same strategy path that the report claims.

Done When:
- `model_run_key` is deterministic for identical git SHA, input fingerprint,
  run config, feature set, training window, seed, execution assumptions, and
  PIT policy versions.
- Invocation UUID or runtime identity is stored as metadata, not part of
  deterministic run identity.
- Label scramble is computed from the same scored walk-forward test dates and
  reported top-half selection logic as the strategy result.
- Falsifier markdown report includes regime breakdown.
- Falsifier markdown report includes label-scramble result.
- Metadata and report evidence agree.
- Focused tests prove deterministic replay, report evidence, and strategy-path
  label scramble behavior.

ARR-56 Contract:
- `model_run_key` is a deterministic digest of contract version, code SHA,
  normalized run config, feature set, joined feature/label input fingerprint,
  model training/test windows, walk-forward scoring config, random seed,
  model-run cost/execution assumptions, and available-at policy versions.
- Fresh invocation metadata, including UUIDs, process ids, wall-clock
  timestamps, host/user names, output paths, report paths, and database
  surrogate ids, is not part of `model_run_key` and must not make the
  `model_runs` or `backtest_runs` create payload differ for the same key.
- If invocation metadata is retained, it is stored outside deterministic run
  identity, for example in an append-only `silver.analytics_runs` backtest row
  whose parameters include the resolved `model_run_key` and `backtest_run_key`.
- `backtest_run_key` is deterministic from the resolved `model_run_key` plus
  normalized backtest evidence config, including label-scramble settings,
  multiple-comparisons setting, cost assumptions, strategy, universe, horizon,
  and contract version.
- Falsifier markdown evidence must include durable model/backtest identity,
  command, git SHA, feature hashes, joined input fingerprint, available-at
  policy versions, random seed, target kind, execution assumptions, model
  windows, data coverage, PIT universe membership, gross/net headline metrics,
  baseline comparison, costs, regime breakdown, label-scramble evidence,
  multiple-comparisons setting, and traceability validation.
- No schema change is required for this contract. Existing `model_runs`,
  `backtest_runs`, and optional append-only `analytics_runs` JSON metadata can
  represent the identity and evidence contract. A later schema ticket is needed
  only if downstream work requires queryable per-invocation foreign keys.

Out Of Scope:
- No new strategy.
- No new vendor ingestion.
- No portfolio or paper-trading execution.
- No unrelated feature-definition changes.
- No automatic schema expansion unless required by the existing registry
  contract.
- No changes to P2 findings for dollar-volume liquidity or FMP failed-response
  raw vaulting.

Guardrails:
- No backtest result may be reported without costs, baselines, and
  reproducibility metadata.
- No prediction or run identity may depend on fresh UUIDs.
- No falsifier evidence may be computed from training-period or unscored rows
  when the reported strategy uses scored test-period rows.
- Preserve point-in-time discipline.
- Keep schema and migration changes out of this Objective unless existing
  registry contracts cannot represent the required metadata.
- Do not commit `.env`, API keys, vendor secrets, or local credentials.

Expected Tickets:
- Define falsifier identity and evidence contract
  Ticket Role: contract
  Dependency Group: falsifier-integrity
  Contracts Touched:
  - falsifier-run-identity
  - falsifier-evidence
  Risk Class: semantic
  Purpose: Define the deterministic run identity inputs and required report
  evidence before implementation starts.
  Expected Impact On Objective: Downstream tickets share one approved contract
  for run keys, invocation metadata, regime evidence, and label-scramble
  evidence.
  Technical Summary: Review `scripts/run_falsifier.py`, report rendering, and
  existing metadata repository expectations; document the exact deterministic
  inputs for `model_run_key` and the user-facing evidence required in markdown.
  Owns:
  - `SPEC.md`
  - `docs/TESTING.md`
  - `docs/PIT_DISCIPLINE.md`
  - `docs/objectives/active/fix-falsifier-reproducibility-evidence.md`
  Do Not Touch:
  - `db/migrations/`
  - `src/silver/features/`
  - `src/silver/sources/`
  Dependencies:
  - none
  Conflict Zones:
  - `SPEC.md`
  - `docs/TESTING.md`
  - `docs/PIT_DISCIPLINE.md`
  Validation:
  - `git diff --check`
  - `python scripts/planning_steward.py --check`
  Proof Packet:
  - deterministic model run identity contract
  - required falsifier report evidence contract
  - no-schema-change or schema-change justification
- Stabilize deterministic model run keys
  Ticket Role: implementation
  Dependency Group: falsifier-integrity
  Contracts Touched:
  - falsifier-run-identity
  Risk Class: semantic
  Purpose: Remove fresh invocation identity from `model_run_key` while keeping
  invocation identity available as metadata.
  Expected Impact On Objective: Re-running the same command at the same git SHA
  with the same input fingerprint and run config creates the same registry
  identity.
  Technical Summary: Update falsifier run identity construction so deterministic
  key hashing uses only stable inputs, and store invocation UUID/runtime identity
  separately in metadata.
  Owns:
  - `scripts/run_falsifier.py`
  - `tests/test_run_falsifier_cli.py`
  - `tests/test_backtest_metadata_repository.py`
  Do Not Touch:
  - `db/migrations/`
  - `src/silver/features/`
  - `src/silver/sources/`
  Dependencies:
  - Define falsifier identity and evidence contract
  Conflict Zones:
  - `scripts/run_falsifier.py`
  - `tests/test_run_falsifier_cli.py`
  - `tests/test_backtest_metadata_repository.py`
  Validation:
  - `python -m pytest tests/test_run_falsifier_cli.py tests/test_backtest_metadata_repository.py`
  - `python scripts/run_falsifier.py --check`
  Proof Packet:
  - deterministic rerun test output
  - sample metadata showing invocation identity outside `model_run_key`
- Align label scramble with reported strategy path
  Ticket Role: implementation
  Dependency Group: falsifier-integrity
  Contracts Touched:
  - falsifier-evidence
  Risk Class: semantic
  Purpose: Make label-scramble evidence test the same scored walk-forward dates
  and top-half selection logic reported by the falsifier strategy result.
  Expected Impact On Objective: Label-scramble pass/fail can no longer be driven
  by training-period, unscored, or otherwise unrelated rows.
  Technical Summary: Build label-scramble payloads from the scored strategy test
  rows used for reported net results, including the reported selection mask.
  Owns:
  - `scripts/run_falsifier.py`
  - `src/silver/backtest/`
  - `tests/test_label_scramble.py`
  - `tests/test_run_falsifier_cli.py`
  Do Not Touch:
  - `db/migrations/`
  - `src/silver/features/`
  - `src/silver/sources/`
  Dependencies:
  - Define falsifier identity and evidence contract
  Conflict Zones:
  - `scripts/run_falsifier.py`
  - `src/silver/backtest/`
  - `tests/test_label_scramble.py`
  Validation:
  - `python -m pytest tests/test_label_scramble.py tests/test_run_falsifier_cli.py`
  - `python scripts/run_falsifier.py --check`
  Proof Packet:
  - test showing scramble payload uses scored test rows
  - test showing top-half selection logic matches reported strategy path
- Add falsifier evidence to markdown reports
  Ticket Role: implementation
  Dependency Group: falsifier-integrity
  Contracts Touched:
  - falsifier-evidence
  - falsifier-report
  Risk Class: semantic
  Purpose: Include regime breakdown and label-scramble result in the user-facing
  falsifier markdown report.
  Expected Impact On Objective: A reviewer can audit required falsifier evidence
  from the report artifact, not only metadata internals.
  Technical Summary: Update report rendering to include regime evidence,
  label-scramble result, and the matching metadata identifiers.
  Owns:
  - `src/silver/reports/falsifier.py`
  - `tests/test_falsifier_report.py`
  - `reports/falsifier/`
  Do Not Touch:
  - `db/migrations/`
  - `src/silver/features/`
  - `src/silver/sources/`
  Dependencies:
  - Define falsifier identity and evidence contract
  Conflict Zones:
  - `src/silver/reports/falsifier.py`
  - `tests/test_falsifier_report.py`
  - `reports/falsifier/`
  Validation:
  - `python -m pytest tests/test_falsifier_report.py`
  - `python scripts/run_falsifier.py --check`
  Proof Packet:
  - rendered report section showing regime breakdown
  - rendered report section showing label-scramble result
- Integrate falsifier metadata and report evidence
  Ticket Role: integration
  Dependency Group: falsifier-integrity
  Contracts Touched:
  - falsifier-run-identity
  - falsifier-evidence
  - falsifier-report
  Risk Class: semantic
  Purpose: Reconcile runner metadata, label-scramble evidence, and markdown
  report output into one coherent falsifier artifact.
  Expected Impact On Objective: Metadata and user-facing report evidence agree
  for the same reported falsifier run.
  Technical Summary: Run the falsifier check path and focused tests across the
  runner, label scramble, metadata repository, and report rendering boundaries;
  repair any drift between implementations.
  Owns:
  - `scripts/run_falsifier.py`
  - `src/silver/reports/falsifier.py`
  - `tests/`
  Do Not Touch:
  - `db/migrations/`
  - `src/silver/features/`
  - `src/silver/sources/`
  Dependencies:
  - Stabilize deterministic model run keys
  - Align label scramble with reported strategy path
  - Add falsifier evidence to markdown reports
  Conflict Zones:
  - `scripts/run_falsifier.py`
  - `src/silver/reports/falsifier.py`
  - `tests/`
  Validation:
  - `python -m pytest tests/test_run_falsifier_cli.py tests/test_label_scramble.py tests/test_falsifier_report.py`
  - `python scripts/run_falsifier.py --check`
  Proof Packet:
  - integrated test output
  - report/metadata agreement evidence
- Validate falsifier reproducibility and evidence integrity
  Ticket Role: validation
  Dependency Group: falsifier-integrity
  Contracts Touched:
  - falsifier-run-identity
  - falsifier-evidence
  - falsifier-report
  Risk Class: semantic
  Purpose: Prove the Objective is complete with focused replay, scramble, and
  report-evidence validation.
  Expected Impact On Objective: The falsifier reproducibility and evidence
  fixes are accepted only when the full path is proven end to end.
  Technical Summary: Run targeted and broad validation, collect proof packets,
  and confirm no unrelated feature, ingestion, or schema behavior changed.
  Owns:
  - `tests/`
  - `scripts/`
  - `docs/`
  Do Not Touch:
  - `db/migrations/`
  - `src/silver/features/`
  - `src/silver/sources/`
  Dependencies:
  - Integrate falsifier metadata and report evidence
  Conflict Zones:
  - `tests/`
  - `scripts/run_falsifier.py`
  - `src/silver/reports/falsifier.py`
  Validation:
  - `git diff --check`
  - `ruff check .`
  - `python -m pytest`
  - `python scripts/run_falsifier.py --check`
  - `python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed`
  Proof Packet:
  - full focused validation output
  - skipped live falsifier reason if `DATABASE_URL` is unavailable
  - final Objective completion summary

Validation:
- `git diff --check`
- `ruff check .`
- `python -m pytest`
- `python scripts/run_falsifier.py --check`
- `python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed`

Conflict Zones:
- `scripts/run_falsifier.py`
- `src/silver/reports/falsifier.py`
- `src/silver/backtest/`
- `tests/test_run_falsifier_cli.py`
- `tests/test_label_scramble.py`
- `tests/test_falsifier_report.py`
- `tests/test_backtest_metadata_repository.py`

## ARR-61 Validation Handoff

Date: 2026-04-30
Branch: `arr-61-validate-falsifier-repro-evidence`
Base SHA after sync: `a690535`

Completion Summary:
ARR-61 validated the falsifier reproducibility and evidence-integrity Objective
end to end through focused replay, label-scramble, report-evidence, metadata,
and broad repository checks. No feature, ingestion, source, schema, or migration
behavior changed.

Focused Validation Output:

```text
$ python -m pytest tests/test_run_falsifier_cli.py tests/test_label_scramble.py tests/test_falsifier_report.py tests/test_backtest_metadata_repository.py
============================= test session starts ==============================
platform darwin -- Python 3.10.17, pytest-8.1.1, pluggy-1.6.0
rootdir: /Users/michael/silver-agent-workspaces/ARR-61
configfile: pyproject.toml
plugins: timeout-2.2.0, anyio-4.9.0, cov-4.1.0, mock-3.12.0, asyncio-0.23.0
asyncio: mode=strict
collected 33 items

tests/test_run_falsifier_cli.py .................                        [ 51%]
tests/test_label_scramble.py ....                                        [ 63%]
tests/test_falsifier_report.py ...                                       [ 72%]
tests/test_backtest_metadata_repository.py .........                     [100%]

============================== 33 passed in 0.94s ==============================
```

Required Validation:

```text
$ git diff --check
<no output; passed>

$ ruff check .
All checks passed!

$ python scripts/run_falsifier.py --check
OK: falsifier CLI check passed for python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed -> reports/falsifier/week_1_momentum.md

$ python -m pytest
============================= 299 passed in 5.34s ==============================
```

Live Falsifier:

```text
$ python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed
error: DATABASE_URL is required unless --check is used. Run `python scripts/bootstrap_database.py` after setting DATABASE_URL, then rerun the falsifier command.
```

Skip Reason:
`DATABASE_URL` was not present in the validation shell, so the DB-backed live
falsifier report run was skipped after capturing the CLI prerequisite failure.
