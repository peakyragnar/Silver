# Silver Symphony Operation

This document defines how Silver uses Symphony to run agentic work. It is a
Silver operating policy, not a definition of Symphony core behavior.

For upstream Symphony concepts, see OpenAI's Symphony article:
[`An open-source spec for Codex orchestration: Symphony`](https://openai.com/index/open-source-codex-orchestration-symphony/).

For local setup commands, see [`SYMPHONY.md`](SYMPHONY.md).

For the portable core design and build stages, see
[`AGENTIC_BUILD_SYSTEM_CORE.md`](AGENTIC_BUILD_SYSTEM_CORE.md).

## Operating Model

Symphony core currently watches Linear, creates isolated workspaces, starts
Codex agents, and keeps active issues moving. Silver's operation layer decides
what work is safe, useful, and ready.

Linear is a current bridge, not the desired machine brain. The correct durable
shape is:

```text
GitHub = code truth
Silver work ledger = work truth
Linear = optional human-facing mirror
```

Use this mental model:

```text
Objective
  -> Objective Compiler
      -> Ticket DAG
          -> Local Ledger
              -> Linear/Symphony mirror
                  -> PRs and proof packets
                      -> Integration Steward or Safety Review
```

Michael reviews Objectives and safety exceptions. The system handles ticket
decomposition, execution, routine repair, merge shepherding, and proof-packet
audit trails after a ticket is approved into `Todo`.

Tickets are not the plan. Tickets are compiled output from an approved
Objective. Symphony runs eligible tickets; it does not decide product direction.

## Roles

| Role | Responsibility |
|---|---|
| Michael | Approves Objectives and Safety Review exceptions. |
| Symphony core | Runs agents for active Linear tickets. |
| Codex builder | Implements one scoped ticket, posts proof, and moves safe completed work to `Merging`. |
| Planning steward | Proposes Objectives and creates guarded tickets. |
| Admission steward | Promotes approved, unblocked `Backlog` tickets into `Todo` within capacity limits. |
| Migration allocator | Serializes and reserves schema migration work. |
| Merge steward | Queues safe green PRs and marks landed work `Done`. |
| Integration steward | Repairs stale/conflicting PRs, reconciles routine drift, and routes semantic exceptions to `Safety Review`. |

Some steward roles may start as manual scripts or manual Codex sessions before
they become unattended automation.

## Current Lane

The current Silver loop is Objective-driven automation through the Linear
bridge:

1. Michael and Codex approve one or more Objectives.
2. Planning creates scoped tickets in Linear `Backlog`.
3. Admission steward promotes safe, unblocked tickets to `Todo`.
4. Symphony assigns agents.
5. Agents implement, open PRs, and post proof packets.
6. Safe completed tickets move to `Merging`.
7. Merge steward queues clean PRs and marks landed tickets `Done`.
8. Failed checks or mechanical conflicts move to `Rework`.
9. Safety exceptions move to `Safety Review`.

This lets Michael steer direction by Objective while the system meters routine
work into active agent capacity. The bridge should be read in bounded calls and
updated only on state changes so Linear never becomes a high-frequency runtime
database.

## Target Lane

The target Silver loop is continuous but bounded:

1. Planning steward proposes the next Objectives from `SPEC.md`, active plans,
   Silver work-ledger state, GitHub PR state, and repo status.
2. Michael approves one or more Objectives.
3. Planning steward decomposes approved Objectives into Silver tickets, mirrored
   to Linear only when the mirror is enabled.
4. Admission steward promotes safe, unblocked tickets to `Todo`.
5. Symphony builds tickets up to the configured concurrency limit.
6. Merge steward lands safe completed PRs.
7. Integration steward repairs stale branches, routine conflicts, and simple
   drift using Objective context.
8. Safety Gate routes destructive, semantic, security, paid/live, or scope-risk
   exceptions to `Safety Review`.
9. Planning steward keeps the queue full from approved Objectives.

The rule is: keep the system full of coherent Objectives, not just busy agents.

## Linear Replacement Boundary

Silver should replace Linear only where Linear is the wrong tool:

| Function | Correct owner | Reason |
|---|---|---|
| Objective approval | Michael plus repo Objective files | Direction should be explicit and auditable. |
| Ticket/runtime state | Silver work ledger | Agents need a cheap, local, rate-limit-free source of truth. |
| Branches, PRs, CI, merge queue | GitHub | Code truth already lives there. |
| Human board/search/comments | Linear, optional | Useful UI, but not required for autonomous operation. |

The migration path is additive:

```text
1. Keep the current Linear bridge working.
2. Stop high-frequency Linear polling.
3. Add a local Silver work ledger.
4. Make stewards read/write the ledger first.
5. Mirror ledger state to Linear only on changes.
6. Delete the Linear dependency only after the ledger path proves better.
```

This protects the current working lane while moving the runtime state out of
Linear.

## Work Ledger

The Silver work ledger is the local source of truth we can poll quickly without
touching Linear. It is additive to the current bridge: it does not replace
Symphony dispatch or Linear mirroring yet.

Default path:

```text
/Users/michael/Silver/.silver/work_ledger.db
```

Override with:

```text
SILVER_LEDGER_PATH=/shared/path/work_ledger.db
```

Initialize it:

```text
python scripts/work_ledger.py init
```

Import approved active Objective files:

```text
python scripts/work_ledger.py import-objectives
```

Inspect local state:

```text
python scripts/work_ledger.py status
python scripts/work_ledger.py list-runnable
python scripts/work_ledger.py events --limit 20
```

Admit local Backlog tickets into the ledger's fast runnable queue:

```text
python scripts/work_ledger.py admit --max-active 5 --ready-buffer 5
```

Preview the Linear mirror:

```text
python scripts/linear_mirror.py
```

Apply the mirror after preview:

```text
python scripts/linear_mirror.py --apply
```

Reconcile post-Symphony PR state back into the ledger:

```text
python scripts/vcs_reconciler.py
python scripts/vcs_reconciler.py --apply
python scripts/integration_steward.py
python scripts/integration_steward.py --apply
```

Then preview and apply the Linear mirror again so the visible board follows the
ledger. The reconciler is conservative: it records PR URL and branch evidence,
marks merged PRs `Done`, moves green open PRs to `Merging`, sends conflicts or
failed required checks to `Rework`, and sends scope or safety exceptions to
`Safety Review`. It is not an automatic code repair worker.

The integration steward writes a repair packet for each `Rework` ticket. The
packet includes PR URL, branch, blocker, allowed scope, protected paths,
validation, and proof-packet refresh requirements. The next Linear mirror
exposes that packet to Symphony so Rework has concrete repair instructions.

Mirror mapping:

| Ledger | Linear bridge |
|---|---|
| `Backlog` | `Backlog` |
| `Ready` | `Todo` |
| `Claimed` / `In Progress` | `In Progress` |
| `Rework` | `Rework` |
| `Merging` | `Merging` |
| `Safety Review` | `Safety Review` |
| `Done` | `Done` |

Move a ticket manually during the MVP:

```text
python scripts/work_ledger.py transition wire-backtest-metadata-registry-001 Ready
```

The ledger stores:

```text
objectives
tickets
ticket roles and dependency groups
contracts touched
ticket_events
runs
conflict_locks
proof_packets
safety_stops
linear_mirror_state
```

Ticket events are the audit trail. Linear sync should eventually read ledger
changes and update Linear only when a visible board state changes.

## Objective

An Objective is the user-facing unit of work. It should be large enough to be
meaningful and small enough to prove.

Objective files live in [`objectives/`](objectives/). Use
[`objectives/TEMPLATE.md`](objectives/TEMPLATE.md) for new Objectives, keep
approved or ready Objectives in [`objectives/active/`](objectives/active/), and
move completed handoffs to [`objectives/completed/`](objectives/completed/).

Good Objective:

```text
Objective:
Prepare the database with identifiers and normalized format for FMP ingestion.

User Value:
Silver can ingest vendor data against durable security identities instead of
fragile ticker-only assumptions.

Done When:
A clean database can store securities, vendor identifiers, raw FMP responses,
and normalized daily prices with point-in-time `available_at` metadata.
```

Poor Objective:

```text
Create five tickets.
```

The second form hides the larger user value and makes it hard for Michael to
know whether the system is moving in the right direction.

## Objective Template

Every Objective should use this template:

```text
Objective:
One clear user-facing outcome.

User Value:
Who benefits and how.

Why Now:
Why this is the next useful chunk.

Done When:
Concrete observable completion criteria.

Out Of Scope:
Work that must not be included.

Guardrails:
Project laws, data safety, permissions, and irreversible actions to avoid.

Expected Tickets:
Compiled ticket DAG with role metadata:

- Ticket title
  Ticket Role: contract | implementation | integration | validation | docs
  Dependency Group: shared contract or feature group name
  Contracts Touched:
  - contract-name
  Risk Class: low | migration | semantic | safety
  Purpose: What this ticket makes true.
  Expected Impact On Objective: How this moves the Objective forward.
  Technical Summary: Concrete implementation mechanism.
  Owns:
  - files, modules, tables, or docs this ticket may edit
  Do Not Touch:
  - files, modules, tables, or docs outside scope
  Dependencies:
  - ticket title or ledger ticket id that must finish first
  Conflict Zones:
  - files, modules, tables, or docs likely to collide
  Validation:
  - commands or artifacts required for this ticket
  Proof Packet:
  - evidence the PR must return for integration and merge review

Validation:
Commands, artifacts, or evidence required before approval.

Conflict Zones:
Files, tables, docs, or workflows likely to collide with parallel work.
```

## Example Objective

```text
Objective:
Prepare database identifiers and normalized format for FMP ingestion.

User Value:
Silver can ingest prices and future FMP artifacts against durable security
identities with point-in-time metadata.

Why Now:
Reliable identifiers are required before scaling ingestion or creating more
features from vendor data.

Done When:
- Identifier schema exists and is migrated.
- Seed reference config can populate the identifier rows.
- FMP price ingest can resolve securities through the durable identifiers.
- Offline checks prove the schema, seed config, and ingest wiring are valid.

Out Of Scope:
- No backtest changes.
- No text features.
- No paper trading.
- No Arrow schema imports.

Guardrails:
- No feature value may be used without an `available_at` rule.
- Do not commit `.env` or vendor secrets.
- Keep schema work in one migration-owner ticket.

Expected Tickets:
- Reserve and add identifier migration.
- Update seed reference config and seeding code.
- Add repository helper for identifier lookup.
- Update FMP ingest to use identifier lookup.
- Add checks/tests/docs for the new ingestion contract.

Validation:
- `git diff --check`
- `python scripts/apply_migrations.py --check`
- `python scripts/seed_reference_data.py --check`
- `python scripts/ingest_fmp_prices.py --check`
- `python -m pytest`
- `ruff check .`

Conflict Zones:
- `db/migrations/`
- `config/seed_reference_data.yaml`
- `scripts/seed_reference_data.py`
- `scripts/ingest_fmp_prices.py`
- `src/silver/reference/`
- `src/silver/ingest/`
```

## Ticket Shape

Tickets are implementation slices created from an approved Objective. A ticket
should be scoped enough that one Codex builder can finish it, validate it, and
produce a reviewable PR.

Each ticket should include:

```text
Ticket Role:
contract | implementation | integration | validation | docs.

Dependency Group:
The shared contract or feature group this ticket belongs to.

Contracts Touched:
Named contracts such as schema, objective-dag, backtest-run-registry, or
falsifier-report.

Risk Class:
low | migration | semantic | safety.

Purpose:
What this ticket makes true.

Parent Objective:
Link or title.

Objective Impact:
One or two user-facing sentences explaining how this work moves the parent
Objective forward.

Technical Summary:
The implementation mechanism, using precise technical language where useful.

Acceptance Criteria:
Concrete checklist.

Owns:
Files, modules, tables, or scripts the agent may edit.

Do Not Touch:
Files or layers intentionally outside scope.

Dependencies:
Tickets or migrations that must land first.

Conflict Zones:
Shared areas that need care.

Validation Required:
Commands and expected artifact paths.

Proof Packet Requirements:
What the merge steward and Michael need in order to audit, repair, or stop.
```

Tickets should not say "improve ingestion" without ownership and acceptance
criteria. That creates vague work and harder reviews.

Good Objective Impact:

```text
Objective Impact:
This standardizes SQL input formatting so FMP extraction rows can be normalized
consistently. It reduces ambiguity before price ingestion writes PIT rows.

Technical Summary:
Adds explicit SQL parameter formatting and validation for normalized FMP daily
price extraction inputs before repository upsert.
```

Poor Objective Impact:

```text
Objective Impact:
Refactor SQL stuff.
```

The goal is not to remove technical detail. The goal is to make every ticket
explain, in Michael-readable language, how a narrow change advances the larger
Objective.

## Linear Bridge State Machine

Silver currently uses these Linear states as a bridge for Symphony. These states
are policy, not just UI. In the target design, the same states belong to the
Silver work ledger and Linear mirrors them.

| State | Symphony active? | Meaning |
|---|---:|---|
| `Backlog` | No | Planned work. Do not start automatically. |
| `Todo` | Yes | Approved and ready for an agent. |
| `In Progress` | Yes | Agent is implementing. |
| `Rework` | Yes | Agent should repair steward feedback, CI failure, or mechanical conflict. |
| `Safety Review` | No | Waiting for Michael because the ticket hit a serious safety or semantic exception. |
| `Merging` | No | Safe completed work; merge steward owns it. |
| `Done` | No | Landed and complete. |
| `Canceled` / `Duplicate` | No | Terminal non-work state. |

Agents should not mark implementation work `Done`. Safe completed tickets post
evidence and move to `Merging`. Tickets that hit destructive, semantic,
paid/live, security, or scope-drift exceptions move to `Safety Review` with a
clear blocker.

## Planning Steward

The planning steward is the Objective and ticket factory. Its job is to create
useful work, not to maximize ticket count.

Inputs:

```text
SPEC.md
docs/index.md
docs/exec-plans/active/*
docs/objectives/active/*
Silver work ledger or current Linear bridge issues
GitHub PRs
repo status
recent proof packets
```

Outputs:

```text
Recommended Objectives
Objective packets
Silver tickets, mirrored to Linear Backlog while the bridge is enabled
dependency notes
conflict-zone notes
migration reservation requests
```

Initial safe mode:

```text
scripts/planning_steward.py --propose
```

The initial implementation is local and propose-only. It reads repository
signals, prefers approved files in `docs/objectives/active/`, and prints
Objective packets with source metadata; it does not write to Linear, GitHub,
the database, or vendors.

Validate local proposal wiring:

```text
scripts/planning_steward.py --check
```

Next mode:

```text
scripts/planning_steward.py --create-backlog --objective <objective-id>
```

Do not begin with unattended ticket creation. First prove that proposed
Objectives and tickets are useful.

## Admission Steward

The admission steward keeps enough safe tickets available for Symphony. It is an
admission controller, not a builder, reviewer, or merger. Its only write is:

```text
Backlog -> Todo
```

Policy:

```text
If active work is below target
and Todo count is below buffer
and an approved Objective has contract-stable runnable tickets
then promote selected tickets from Backlog to Todo.
```

Admission is not "one ticket at a time." It is contract-gated parallelism:

```text
contract tickets first
then parallel implementation tickets
then integration tickets
then validation tickets
```

Multiple Objectives may run at once when their `contracts_touched` and hard
conflict zones do not overlap.

Default operating target:

```text
max active agents: 5
Todo buffer: 5 to 10
```

Do not try to avoid every possible conflict. Normal branch, test, docs, and
shared-code conflicts should flow through `Rework`. The steward should block
only hard conflicts:

```text
unfinished blocking relation
unfinished contract in the same dependency group
integration ticket waiting on implementation output
validation ticket waiting on integration output
open PR already exists for the same issue
one active migration/schema-owner lane
overlapping active contract change
same high-risk steward/workflow file
Safety Review required before start
```

Preview:

```text
python scripts/admission_steward.py --dry-run --max-active 5 --todo-buffer 5
```

Promote once:

```text
python scripts/admission_steward.py --promote --max-active 5 --todo-buffer 5
```

Watch mode:

```text
python scripts/admission_steward.py --watch --promote --max-active 5 --todo-buffer 5 --poll-interval 300
```

## Migration Lane

Migration handling is deterministic at the coordination layer and agentic at the
schema-design layer.

Deterministic:

```text
who may create a migration
which migration number/name is reserved
which tickets are blocked by the migration
which checks must pass
how number conflicts are repaired
```

Agentic:

```text
what schema shape is correct
which constraints preserve point-in-time safety
how to migrate existing data safely
how repositories, tests, and docs should adapt
```

Default rule:

```text
Only one active Todo ticket may own `db/migrations/`.
```

When an Objective needs schema work:

1. Planning steward identifies schema work.
2. Migration allocator reserves the next migration number and name.
3. One schema-owner ticket is created.
4. Dependent tickets remain in `Backlog` or are explicitly blocked.
5. Schema-owner PR lands before dependent implementation tickets enter `Todo`,
   unless the Objective states a safe parallel contract.

Example reservation:

```text
Reserved migration:
005_fmp_identifiers.sql

Owner ticket:
ARR-41 Prepare FMP identifier schema

Blocked tickets:
ARR-42 Add identifier repository helper
ARR-43 Update FMP ingest normalization
ARR-44 Add identifier seed validation
```

Number-only migration conflicts may be repaired automatically by renumbering to
the next available migration and updating references. Semantic schema conflicts
must be routed to `Rework` with a summary.

Semantic conflict examples:

```text
two PRs define different meanings for the same table
one PR adds a column another PR removes or renames
constraints change PIT behavior
available_at rules conflict
data-retention behavior changes
```

## Integration Steward

The integration steward handles routine post-Symphony repair in Objective
context. It covers stale branches, failed merge queue attempts, merge conflicts,
simple wiring drift between related tickets, and proof-packet refreshes.

Current MVP:

```text
scripts/vcs_reconciler.py
scripts/integration_steward.py
```

The MVP is detection, routing, and repair-packet creation. It reads GitHub PR
state through the VCS adapter, matches PRs to ledger tickets, records branch/PR
evidence, updates ledger status, and writes the bounded instructions a repair
worker needs. Full automatic branch editing comes after this lane is proven.

Mechanical conflicts may be repaired automatically:

```text
documentation context conflicts
formatting-only conflicts
test import ordering
migration number-only conflicts
lockfile refresh caused by accepted dependency change
```

Semantic conflicts must go to `Safety Review` when they change the approved
Objective contract or require Michael's judgment:

```text
schema meaning
point-in-time behavior
label availability
feature definition semantics
backtest metric definitions
data deletion or retention
security/secrets behavior
```

Conflict repair flow:

1. Detect conflict or failed merge queue attempt.
2. Identify PR, ticket, changed files, and conflict files.
3. Classify mechanical vs semantic.
4. For mechanical conflicts, update branch, repair, run validation, and refresh
   the proof packet.
5. For semantic conflicts, move the ticket to `Safety Review` with a clear
   summary.

Repair or exception summary should include:

```text
Objective:
Ticket:
PR:
Conflict files:
Likely cause:
Why this is mechanical or semantic:
Allowed repair scope:
Validation required:
```

## Merge Steward

The merge steward owns `Merging`.

It should:

1. Read work-ledger tickets in `Merging` or current Linear bridge issues.
2. Find the matching GitHub PR.
3. Confirm the PR still matches the approved ticket scope.
4. Confirm required checks are passing.
5. Add the PR to the GitHub merge queue.
6. Mark the issue `Done` after merge.
7. Move failed checks or conflicts to `Rework`.

It should not:

```text
bypass the merge queue
rewrite history
approve its own implementation
change product scope
silently resolve semantic conflicts
```

Current command references live in [`SYMPHONY.md`](SYMPHONY.md).

## Proof Packets

Every safe completed implementation ticket must include a Linear comment headed:

```text
## Proof Packet
```

Required contents:

```text
PR link
parent Objective
ledger ticket ID
ticket role
dependency group
contracts touched
risk class
Objective Impact summary
changed files summary
acceptance criteria status
validation commands and outcomes
CI status or link
risks, assumptions, and known gaps
generated artifact path or link, when relevant
exact blocker, when incomplete or routed to `Safety Review`
```

Proof packets are audit receipts and steward inputs. They are not routine
approval requests. A packet that only makes prose claims is insufficient for
`Merging`.

The Objective Impact summary should answer:

```text
What part of the Objective did this ticket advance?
What can Silver do now, or do more safely, because this landed?
What remains for the Objective after this ticket?
```

Example:

```text
Objective Impact:
This ticket gives the FMP ingestion Objective a stable SQL input contract for
normalized daily-price extraction. The next ticket can use this contract to
persist normalized rows without redefining extraction shape.
```

Silver-specific proof usually includes:

```text
git diff --check
python scripts/bootstrap_database.py --check
python scripts/apply_migrations.py --check
python scripts/seed_available_at_policies.py --check
python scripts/seed_reference_data.py --check
python scripts/seed_trading_calendar.py --check
python scripts/check_falsifier_inputs.py --check
python -m pytest
ruff check .
```

Tickets should run the narrowest meaningful checks while iterating and broader
checks before handoff.

## Safety Review

`Safety Review` is the only human stop after a ticket enters `Todo`.
Use it when automation cannot safely decide whether the change remains within
the approved Objective and ticket scope.

Safety Review is required for:

```text
data deletion or destructive migration
PIT rule changes
feature, label, or backtest metric semantic conflicts
secret or credential handling changes
new paid or live external service behavior
ticket scope drift
automation permission expansion
ambiguous schema meaning
```

Safety Review is not required for routine stale branches, mechanical merge
conflicts, formatting conflicts, failed checks with clear fixes, or proof-packet
refreshes. Those move through `Rework`.

## Automation Ladder

Do not jump straight to overnight autonomy. Increase automation only after the
previous rung produces good evidence.

1. Manual Objective writing.
2. Planning steward proposes Objectives and tickets.
3. Planning steward creates tickets in `Backlog`.
4. Admission steward promotes approved, low-risk tickets to `Todo`.
5. Merge steward continuously handles safe completed PRs.
6. Integration steward repairs routine conflicts and drift.
7. Overnight mode keeps `Todo` topped up from approved Objectives.
8. Michael review focuses on Objective outcomes and exception paths.

Human approval remains required for:

```text
new Objective approval
semantic schema conflicts
PIT rule changes
data deletion
secret handling changes
expanding live or paid external service usage
moving unproven automation to a higher rung
```

## Objective Store

Objective flow uses a small file-based store:

```text
docs/objectives/active/
docs/objectives/completed/
```

Each Objective file should use the Objective template above. Tickets should link
back to the Objective file or parent issue, whether they live in the future
Silver ledger or the current Linear bridge.

Do not create a large planning database until the file-based flow is painful.

## Bootstrap Status

The first operating-system Objectives are now complete or in place:

```text
Planning steward:
Creates or proposes Objective-backed tickets in Backlog.

Admission steward:
Promotes approved, unblocked Backlog tickets into Todo up to capacity.

Merge steward:
Queues safe completed PRs, marks landed issues Done, and routes failed checks or
conflicts to Rework or Safety Review.
```

The remaining maturity work is to make conflict repair more agentic and to add
better evidence dashboards for overnight runs. The core lane is already:

```text
Objective approval -> Backlog tickets -> Admission -> Symphony -> Merging -> Done/Rework/Safety Review
```

## User Checklist

When operating Silver through Symphony, Michael should ask:

1. Is the Objective clear enough to approve?
2. Does it say what is out of scope?
3. Are conflict zones visible?
4. Does each ticket explain how it advances the Objective?
5. Is there at most one active migration-owner ticket?
6. Are low-risk tickets the only ones promoted automatically?
7. Do proof packets contain commands, outcomes, and Objective impact?
8. Are semantic conflicts routed to `Rework` instead of patched silently?
9. Is the system producing useful progress, not just more tickets?

If the answer to any of these is no, pause automation at the current rung and
fix the operating policy before adding more agents.
