# Silver Documentation Index

`AGENTS.md` is the short map. This directory is the system of record for agents
and humans building Silver.

## Core Docs

- [`../SPEC.md`](../SPEC.md): product contract and build phases
- [`ARCHITECTURE.md`](ARCHITECTURE.md): repository structure and layer rules
- [`AI_NATIVE_SILVER.md`](AI_NATIVE_SILVER.md): conceptual model for Silver as
  an AI-native investment learning environment
- [`PIT_DISCIPLINE.md`](PIT_DISCIPLINE.md): time, `available_at`, and lookahead
  rules
- [`PHASE1_RUNBOOK.md`](PHASE1_RUNBOOK.md): local Phase 1 preflight and
  falsifier runbook
- [`TESTING.md`](TESTING.md): validation ladder and phase gates
- [`SECURITY.md`](SECURITY.md): secrets, vendor data, and local environment
- [`SYMPHONY.md`](SYMPHONY.md): how to run Symphony against this repository

## Plans

- [`exec-plans/active/`](exec-plans/active/): current implementation plans
- [`exec-plans/completed/`](exec-plans/completed/): completed plans and handoffs

## Documentation Rule

When behavior, architecture, validation, or orchestration changes, update the
smallest relevant doc in the same change.
