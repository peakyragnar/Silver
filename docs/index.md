# Silver Documentation Index

`AGENTS.md` is the short map. This directory is the system of record for agents
and humans building Silver.

## Core Docs

- [`../SPEC.md`](../SPEC.md): product contract and build phases
- [`ARCHITECTURE.md`](ARCHITECTURE.md): repository structure and layer rules
- [`FUNDAMENTALS_V0.md`](FUNDAMENTALS_V0.md): first FMP normalized fundamentals
  scope, selected metrics, diluted-share rule, and SEC audit relationship
- [`EARNINGS_RELEASE_EVENTS_V0.md`](EARNINGS_RELEASE_EVENTS_V0.md): SEC 8-K
  Item 2.02 release-event clock and linking to income-statement fundamentals
- [`AI_NATIVE_SILVER.md`](AI_NATIVE_SILVER.md): conceptual model for Silver as
  an AI-native investment learning environment
- [`PIT_DISCIPLINE.md`](PIT_DISCIPLINE.md): time, `available_at`, and lookahead
  rules
- [`PHASE1_RUNBOOK.md`](PHASE1_RUNBOOK.md): local Phase 1 preflight and
  falsifier runbook
- [`TESTING.md`](TESTING.md): validation ladder and phase gates
- [`SECURITY.md`](SECURITY.md): secrets, vendor data, and local environment
- [`SYMPHONY.md`](SYMPHONY.md): how to run Symphony against this repository
- [`Symphony-Operation.md`](Symphony-Operation.md): how Silver uses Symphony
  Objectives, tickets, stewards, migrations, and conflicts
- [`AGENTIC_BUILD_SYSTEM_CORE.md`](AGENTIC_BUILD_SYSTEM_CORE.md): portable
  objective-to-ticket orchestration core design and implementation stages
- [`OBJECTIVE_CONTROLLER_SCHEMATIC.md`](OBJECTIVE_CONTROLLER_SCHEMATIC.md):
  readable system map for the objective controller, ledger, Linear mirror,
  Symphony, GitHub, repair, merge, and safety flow
- [`../scripts/work_ledger.py`](../scripts/work_ledger.py): local Objective and
  ticket ledger CLI for fast orchestration state
- [`../scripts/linear_mirror.py`](../scripts/linear_mirror.py): mirrors local
  ledger ticket state to Linear for current Symphony visibility
- [`../scripts/objective_run.py`](../scripts/objective_run.py): portable
  objective run controller that composes the ledger, Linear/Symphony mirror,
  VCS reconciler, repair lane, and merge steward
- [`../scripts/research_results_report.py`](../scripts/research_results_report.py):
  one-page markdown cockpit for latest hypothesis, feature, backtest, verdict,
  and next-test evidence
- [`../config/agentic_build.yaml`](../config/agentic_build.yaml): Silver project
  adapter config for the portable objective controller

## Plans

- [`objectives/TEMPLATE.md`](objectives/TEMPLATE.md): required Objective fields
  for approved work chunks before ticket decomposition
- [`objectives/active/`](objectives/active/): Objectives approved or ready for
  decomposition into implementation tickets
- [`objectives/completed/`](objectives/completed/): completed Objective
  handoffs
- [`exec-plans/active/`](exec-plans/active/): current implementation plans
- [`exec-plans/completed/`](exec-plans/completed/): completed plans and handoffs

## Documentation Rule

When behavior, architecture, validation, or orchestration changes, update the
smallest relevant doc in the same change.
