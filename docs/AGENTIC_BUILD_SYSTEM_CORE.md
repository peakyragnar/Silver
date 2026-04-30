# Agentic Build System Core

Status: Design plan

Purpose: Define the portable objective-to-ticket orchestration layer that sits
above Symphony-style agent runners and below a project-specific product plan.

This document is intentionally portable. Silver is the first proving repo, not
the shape of the system. Silver-specific rules belong in a project adapter.

For upstream Symphony context, see OpenAI's article:
[`An open-source spec for Codex orchestration: Symphony`](https://openai.com/index/open-source-codex-orchestration-symphony/).

## Decision Anchor

Goal:

Build a portable agentic build system that lets a human approve clear
Objectives, then compiles those Objectives into a ticket DAG, dispatches safe
runnable work to agents, integrates routine PR output, and stops only for true
exceptions.

User value:

Michael can steer by product/design intent instead of managing one ticket, one
agent, or one merge conflict at a time.

Constraints:

- The core must work in different repositories.
- The core must not depend on Silver, Linear, GitHub, or one Symphony runner.
- Project-specific rules must be adapter data, not core behavior.
- The local ledger must be the source of truth for objective and ticket state.
- Human review must remain mandatory for destructive, semantic, security,
  paid/live, or scope-risk cases.

Evidence:

- Symphony turns tracker issues into running agent sessions and reduces manual
  session management.
- Ticket-by-ticket execution still leaves a planning gap before tickets and an
  integration gap after PRs.
- Real teams handle parallel work through contracts, ownership boundaries,
  release discipline, and integration engineers.

Falsifier:

This design is wrong if Objectives cannot be compiled into useful DAGs, if most
work still requires Michael to manually schedule or reconcile routine tickets,
or if the core becomes so Silver-specific that another repository cannot adopt
it through adapters.

## Core Distinction

The missing concept is:

```text
Tickets are not the plan.
Tickets are compiled output from an Objective.
```

The system has three different layers:

| Layer | Responsibility | Must not do |
|---|---|---|
| Objective compiler | Turns user-approved Objectives into a ticket DAG. | Run agents or merge PRs. |
| Symphony pipes | Run agents for runnable tickets and return PRs/proof packets. | Decide product direction. |
| Integration layer | Reads PR output in Objective context and handles routine landing work. | Approve semantic or safety exceptions. |

Keeping these separate is what makes the architecture portable.

## Portable Architecture

```text
Objective
  user-approved product/design goal

Objective Compiler
  converts objective into ticket DAG

Local Ledger
  source of truth for objectives, tickets, dependencies, and states

Dispatch Adapter
  mirrors runnable tickets to Linear/Symphony or another runner

Runner Adapter
  starts or observes agent work for runnable tickets

VCS Adapter
  reads PRs, checks, changed files, mergeability, and proof evidence

Integration Steward
  resolves routine conflicts using objective context

Safety Gate
  stops for destructive, semantic, security, paid/live, or scope-risk cases
```

## Core And Adapters

The core should be named and treated as a portable build system:

```text
Agentic Build System Core
  objective model
  objective compiler
  ticket DAG model
  local ledger
  runner adapter interface
  tracker adapter interface
  VCS adapter interface
  merge/integration policy
  safety policy
  proof packet schema

Silver Project Adapter
  SPEC.md
  PIT rules
  schema rules
  backtest rules
  FMP ingestion context
  Silver validation commands
  Silver safety classifiers
```

Adapter examples:

| Adapter | First implementation | Portable alternative |
|---|---|---|
| Runner adapter | Symphony | Another agent runner or local worker pool |
| Tracker adapter | Linear mirror | GitHub Issues, Jira, local files, no tracker |
| VCS adapter | GitHub | GitLab, local git, Gerrit |
| Project adapter | Silver rules | Any repo's `SPEC.md`, tests, safety rules |

Linear becomes one mirror. Symphony becomes one runner. GitHub becomes one VCS
adapter. Silver becomes one customer.

## Objective Model

An Objective is the human-approved unit of direction.

Required fields:

```text
objective_id
title
user_value
why_now
done_when
out_of_scope
guardrails
validation
conflict_zones
project_adapter
```

Optional fields:

```text
priority
risk_class
deadline
owner
source_refs
```

Objectives should be meaningful product or system outcomes, not a bag of
tickets.

Good:

```text
Objective: Build the portable objective-to-ticket orchestration core.
```

Poor:

```text
Objective: Create five tickets.
```

## Ticket DAG Model

Each Objective compiles into tickets with roles and dependencies.

Required ticket metadata:

```text
objective_id
ticket_id
ticket_role: contract | implementation | integration | validation | docs
dependency_group
contracts_touched
risk_class
expected_impact_on_objective
dependencies
owns
do_not_touch
validation
proof_packet
```

Ticket roles:

| Role | Purpose | Admission rule |
|---|---|---|
| `contract` | Define or change shared schemas, APIs, interfaces, data semantics, or ownership boundaries. | Run before dependent work. Serialize when contracts overlap. |
| `implementation` | Build against accepted contracts. | Fan out after required contracts are stable. |
| `integration` | Reconcile implementations, adapt wiring, resolve routine drift, and prepare landing. | Run after implementation group is complete or PRs are available. |
| `validation` | Prove the Objective outcome end to end. | Run after integration. |
| `docs` | Update docs, runbooks, or handoff material. | Can run in parallel when it does not alter contracts. |

Default Objective shape:

```text
contract ticket(s)
  -> parallel implementation ticket group(s)
      -> integration ticket(s)
          -> validation ticket(s)
```

Multiple Objectives can run in parallel when their contracts and conflict zones
do not overlap.

## Contract-Gated Parallelism

The admission rule is not "one ticket at a time." It is:

```text
Admit multiple tickets only when their required contracts are stable.
```

The ledger should ask:

```text
Does this ticket create or change a shared contract?
Does another active ticket depend on that contract?
Does another Objective touch the same contract?
Is this an integration ticket that must wait for implementation output?
Is this a validation ticket that must wait for integration?
Is this independent enough to run now?
```

This gives the system real parallelism without letting each agent invent the
shared interface independently.

## PR Metadata Contract

Every PR produced by a compiled ticket should carry objective metadata in its
description or proof packet:

```text
objective_id
ticket_id
ticket_role
dependency_group
contracts_touched
risk_class
proof_packet
expected_impact_on_objective
```

The VCS adapter reads this metadata so the Integration Steward can reason about
PRs as Objective output, not isolated branches.

## Proof Packet Schema

Each completed ticket should return a proof packet with:

```text
objective_id
ticket_id
ticket_role
pr_url
changed_files
contracts_touched
acceptance_criteria
validation_commands
validation_results
ci_status
expected_impact_on_objective
risks
known_gaps
artifacts
```

Proof packets are not prose congratulations. They are machine-readable audit
receipts and inputs to the integration layer.

## Integration Steward

The Integration Steward is agentic. It uses Objective context to repair routine
post-Symphony work.

It handles:

- stale branches
- routine code conflicts
- dependency drift after a contract lands
- simple wiring mismatch between related tickets
- CI retries for known flaky or environmental failures
- final branch adaptation before merge

It does not approve safety exceptions.

Decision table:

| Situation | Route |
|---|---|
| Routine code conflict | Integration Steward fixes or rebases. |
| Stale branch | Integration Steward updates. |
| Implementation changed contract unexpectedly | Safety or Design Review. |
| Two Objectives touch the same contract | Serialize contract change, then parallelize downstream work. |
| Destructive migration | Safety Review. |
| Semantic data rule change | Safety Review. |
| Security or secret-handling change | Safety Review. |
| Paid/live external side effect | Safety Review. |
| Scope expansion outside Objective | Safety or Design Review. |

This separates routine engineering repair from true human judgment.

## State Model

Objective states:

```text
Draft
Approved
Compiled
In Progress
Integration
Validation
Done
Canceled
```

Ticket states:

```text
Backlog
Ready
Claimed
In Progress
Rework
Merging
Safety Review
Done
Canceled
```

PR states:

```text
Open
Checks Pending
Checks Failed
Conflicted
Needs Integration
Ready To Merge
Merged
Closed
```

The ledger owns Objective and ticket state. The VCS adapter reports PR state.
Tracker adapters mirror only the states that humans or runners need to see.

## Safety Policy

The Safety Gate should stop only for true exceptions:

- destructive data or migration operations
- semantic changes to data, labels, features, backtests, or user-facing claims
- security, secrets, permissions, or credential handling
- paid/live vendor or production side effects
- scope risk outside the approved Objective
- unclear ownership of a shared contract

Everything else should first be evaluated for routine integration.

## Implementation Stages

### Stage 0: Design Record And Boundaries

Outcome:

The repo has a clear design document that names the portable core and separates
it from Silver-specific operation.

Deliverables:

- Portable architecture document.
- Docs index link.
- Current-state inventory of existing Silver scripts and docs.

Done when:

- Michael can point to one document that explains the system and its build
  stages.
- No code behavior changes are required in this stage.

### Stage 1: Objective Schema And Template

Outcome:

Objective files can describe tickets as compiled output with roles, dependency
groups, contracts touched, expected impact, risk, and validation.

Deliverables:

- Update Objective template.
- Add example portable Objective for the orchestration core.
- Define required ticket metadata.

Done when:

- A human-readable Objective can be reviewed without reading Linear.
- The expected ticket graph is explicit enough to compile into a DAG.

### Stage 2: Objective Compiler MVP

Outcome:

The planning steward parses Objective files into structured ticket proposals.

Deliverables:

- Parse `ticket_role`.
- Parse `dependency_group`.
- Parse `contracts_touched`.
- Parse `risk_class`.
- Preserve `expected_impact_on_objective`.
- Emit deterministic ticket IDs.

Done when:

- The compiler can produce the same ticket DAG from the same Objective input.
- Missing metadata is either inferred conservatively or flagged for review.

### Stage 3: Portable Ledger DAG

Outcome:

The local ledger becomes the source of truth for Objective and ticket graph
state.

Deliverables:

- Store Objective state.
- Store ticket role, dependency group, contracts touched, and risk class.
- Store dependency edges.
- Store proof packet references.
- Keep tracker IDs as mirror metadata, not primary identity.

Done when:

- Ledger state can answer "what is runnable now and why?"
- Linear can be rebuilt from ledger state.
- The ledger does not require Silver-specific concepts.

### Stage 4: Contract-Gated Admission

Outcome:

Admission allows useful parallelism while preventing shared-contract chaos.

Deliverables:

- Admit contract tickets first.
- Fan out implementation tickets after required contracts are stable.
- Hold integration tickets until implementation output exists.
- Hold validation tickets until integration is complete.
- Serialize overlapping contract changes across Objectives.

Done when:

- Multiple independent Objectives can run at once.
- Dependent tickets inside one Objective run in the intended order.
- The system can explain every skip/admit decision.

### Stage 5: Dispatch And Tracker Adapters

Outcome:

Runnable ledger tickets can be mirrored to a runner-facing surface without
making that surface the brain.

Deliverables:

- Linear mirror emits objective/ticket metadata.
- Runner adapter interface is defined.
- Symphony dispatch remains the first runner implementation.
- Tracker mirror updates only on visible state changes.

Done when:

- Symphony can keep running from Linear while ledger remains authoritative.
- Another tracker or no tracker can be added without redesigning the core.

### Stage 6: VCS Adapter And PR Metadata

Outcome:

The system reads PR output in Objective context.

Deliverables:

- Define PR metadata contract.
- Read PR link, checks, mergeability, changed files, and proof packet.
- Associate PRs with Objective and ticket IDs.
- Classify PRs as routine merge, integration, rework, or safety review.

Done when:

- A PR can be evaluated without guessing which Objective it belongs to.
- Changed files can be compared against `owns`, `do_not_touch`, and
  `contracts_touched`.

Current Silver MVP:

- `scripts/vcs_reconciler.py` reads GitHub PR state, matches PRs to ledger
  tickets, records PR URL and branch evidence, and updates ledger states.
- The MVP routes merged PRs to `Done`, safe green open PRs to `Merging`,
  conflicts and failed required checks to `Rework`, and safety exceptions to
  `Safety Review`.
- `scripts/integration_steward.py` turns `Rework` tickets into repair packets
  with PR URL, branch, blocker, allowed scope, protected paths, validation, and
  proof refresh requirements.
- `scripts/integration_repair_runner.py` can prepare an isolated worktree,
  merge current `main` into the PR branch, run an optional repair-agent command,
  run validation, push the repaired branch, and move the ticket back to
  `Merging`.
- It does not force-push, rewrite history, or silently resolve semantic/safety
  exceptions.

### Stage 7: Integration Steward MVP

Outcome:

Routine post-Symphony repair no longer defaults to Michael.

Deliverables:

- Detect stale branches.
- Detect routine conflicts.
- Write bounded repair packets for agentic integration workers.
- Execute bounded branch repair from repair packets.
- Re-run required validation before moving repaired tickets back to `Merging`.
- Return unresolved semantic or safety exceptions to Safety Review.

Done when:

- Simple stale-branch repair can be executed by the system.
- Content-conflict repair has an explicit agent-command lane and bounded packet.
- Michael sees only exceptions that require human judgment.

### Stage 8: Safety Gate Hardening

Outcome:

The system reserves human attention for real risk.

Deliverables:

- Portable safety categories.
- Project-adapter safety classifiers.
- Safety Review comments with exact blocker.
- Ledger audit trail for stop decisions.

Done when:

- Safety stops are explainable, sparse, and tied to Objective context.
- Routine repair and true exception handling are clearly separated.
- Planned contract tickets can pass additive docs-only contract clarifications
  inside their declared ownership while preserving Safety Review for code,
  deletion, destructive, secret, paid/live, or scope-drift risk.

### Stage 9: Portability Extraction

Outcome:

The core can be adopted by another repository with a project adapter.

Deliverables:

- Core schema docs.
- Adapter interface docs.
- Minimal second-repo fixture or dry-run harness.
- Silver adapter documented separately.

Done when:

- A new repo can define its own Objective template additions, validation
  commands, safety policy, runner adapter, and VCS adapter.
- Silver-specific words are absent from core code paths except in the Silver
  adapter.

## Initial Silver Implementation Slice

The first practical slice should be small:

1. Update Objective template with portable ticket metadata.
2. Teach the planning steward to parse that metadata.
3. Store it in the ledger.
4. Gate admission by role, dependency group, and contracts touched.
5. Mirror metadata to Linear ticket descriptions.
6. Update proof packet requirements.
7. Add tests for contract-first fanout and integration gating.

This slice does not build semantic conflict resolution. It makes the work graph,
repair packet, and bounded branch-repair lane real first.

Current Silver MVP paths:

| Capability | Path |
|---|---|
| Objective ticket metadata parsing | `scripts/planning_steward.py` |
| Local ledger DAG metadata and admission gates | `scripts/work_ledger.py` |
| Linear mirror metadata surface | `scripts/linear_mirror.py` |
| VCS PR reconciliation and routing | `scripts/vcs_reconciler.py` |
| Integration repair packets | `scripts/integration_steward.py` |
| Bounded integration repair runner | `scripts/integration_repair_runner.py` |
| Objective template | `docs/objectives/TEMPLATE.md` |
| Silver operation policy | `docs/Symphony-Operation.md` |
| Focused tests | `tests/test_planning_steward.py`, `tests/test_work_ledger.py`, `tests/test_linear_mirror.py`, `tests/test_vcs_reconciler.py`, `tests/test_integration_steward.py`, `tests/test_integration_repair_runner.py` |

## Defer For Now

Do not build these before the graph is trustworthy:

- a new web UI
- a replacement for Symphony
- a replacement for GitHub
- fully automatic semantic conflict resolution
- high-frequency Linear polling
- Silver-only assumptions in the core

## Success Criteria

The system is working when:

- Michael approves Objectives, not individual busywork tickets.
- Objectives compile into a visible DAG.
- The local ledger is the source of truth.
- Linear is optional mirror state.
- Symphony receives runnable tickets, not product authority.
- PRs carry objective/ticket metadata.
- Routine conflicts go to Integration Steward.
- Safety Review is reserved for true exceptions.
- The same core can run in another repository through adapters.
