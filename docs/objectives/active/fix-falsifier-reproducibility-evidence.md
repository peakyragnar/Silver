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
