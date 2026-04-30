# Objective Controller Schematic

Status: Reference

Purpose: Give Michael and future agents a readable map of how Silver's
objective-aware orchestration works now.

Core rule:

```text
Symphony still runs the agents.
The objective run controller runs the system around Symphony.
```

This system extends Symphony. It does not replace Symphony.

```text
                                MICHAEL
                                  |
                                  v
                    Approves a clear Objective
                                  |
                                  v
+------------------------------------------------------------------+
|                    OBJECTIVE RUN CONTROLLER                       |
|                    scripts/objective_run.py                       |
|                                                                  |
|  The controller is the coordinator. It does not replace Symphony. |
|  It runs the loop: import -> admit -> mirror -> observe ->        |
|  reconcile -> repair/merge/safety -> repeat.                     |
+------------------------------------------------------------------+
        |                  |                    |                 |
        v                  v                    v                 v
+---------------+   +---------------+    +---------------+   +---------------+
| Objective     |   | Local Work    |    | GitHub / VCS  |   | Project       |
| Compiler      |   | Ledger        |    | Reconciler    |   | Adapter       |
|               |   |               |    |               |   |               |
| Turns the     |   | Source of     |    | Reads PRs,    |   | Silver rules: |
| Objective     |   | truth for     |    | checks,       |   | tests, PIT,   |
| into a ticket |   | objectives,   |    | changed files,|   | safety, docs, |
| DAG.          |   | tickets,      |    | mergeability. |   | validation.   |
|               |   | dependencies, |    |               |   |               |
| contract ->   |   | states.       |    | Writes PR     |   | Portable: a   |
| implementation|   |               |    | evidence back |   | new repo swaps|
| -> integration|   | Linear is NOT |    | to the ledger.|   | this adapter. |
| -> validation |   | the brain.    |    |               |   |               |
+---------------+   +---------------+    +---------------+   +---------------+
        |                  ^
        v                  |
+------------------------------------------------------------------+
|                          ADMISSION GATE                           |
|                                                                  |
|  Decides what is runnable now.                                   |
|                                                                  |
|  Rules:                                                          |
|  - contract tickets first                                        |
|  - implementation tickets fan out after contracts are stable      |
|  - integration waits for implementation output                    |
|  - validation waits for integration                               |
|  - overlapping contract changes are serialized                    |
+------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------+
|                         LINEAR MIRROR                             |
|                                                                  |
|  Linear is only the dispatch/mirror surface that Symphony already |
|  knows how to consume.                                            |
|                                                                  |
|  Ledger Ready       -> Linear Todo                                |
|  Ledger In Progress -> Linear In Progress                         |
|  Ledger Rework      -> Linear Rework                              |
|  Ledger Merging     -> Linear Merging                             |
|  Ledger Done        -> Linear Done                                |
+------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------+
|                         SYMPHONY PIPES                            |
|                                                                  |
|  Symphony remains the execution engine.                           |
|                                                                  |
|  It watches runnable Linear tickets, creates isolated workspaces,  |
|  launches Codex agents, tracks sessions, handles retries/stalls,   |
|  and returns PRs/proof packets.                                   |
+------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------+
|                         CODEX AGENT                               |
|                                                                  |
|  One scoped ticket.                                               |
|  One branch/workspace.                                            |
|  Implements within ticket ownership boundaries.                   |
|  Posts proof packet.                                              |
|  Opens or updates GitHub PR.                                      |
+------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------+
|                         GITHUB PR                                 |
|                                                                  |
|  Code truth: changed files, checks, branch state, mergeability.   |
+------------------------------------------------------------------+
        |
        v
+------------------------------------------------------------------+
|                         VCS RECONCILER                            |
|                                                                  |
|  Reads PRs in Objective context.                                  |
|                                                                  |
|  If PR is merged:                                                 |
|    -> ledger Done                                                 |
|                                                                  |
|  If PR is green and mergeable:                                    |
|    -> ledger Merging                                              |
|                                                                  |
|  If PR has failed checks or routine conflicts:                    |
|    -> ledger Rework                                               |
|                                                                  |
|  If PR has destructive, semantic, security, paid/live, or scope    |
|  risk:                                                           |
|    -> Safety Review                                               |
+------------------------------------------------------------------+
        |
        v
+------------------------+------------------------+------------------------+
|                        |                        |                        |
v                        v                        v                        v
MERGE STEWARD            INTEGRATION STEWARD      SAFETY REVIEW            LEDGER
                         + REPAIR RUNNER

Green PRs:               Routine conflict:        True exception:          State is
queue merge,             write repair packet,     stop for Michael.        updated.
mark Done after          optionally run bounded                            Controller
GitHub merge.            repair and validation.                            loops again.
```

Short version:

```text
Michael approves Objective
        |
Objective Run Controller
        |
Local Ledger decides runnable DAG tickets
        |
Linear mirrors runnable tickets
        |
Symphony runs Codex agents
        |
GitHub PRs come back
        |
Controller reconciles PRs
        |
merge / repair / safety stop
        |
ledger advances next DAG layer
        |
Objective completes
```

## Component Boundaries

| Component | Responsibility | Must not do |
|---|---|---|
| Objective Run Controller | Coordinates the loop around Symphony. | Replace Symphony or choose product direction without an approved Objective. |
| Local Work Ledger | Source of truth for Objective, ticket, dependency, and state. | Treat Linear as authoritative runtime state. |
| Linear Mirror | Makes runnable ledger tickets visible to Symphony. | Become the planning brain. |
| Symphony Pipes | Runs Codex agents in workspaces for active tickets. | Decide Objective direction or merge policy. |
| GitHub / VCS Reconciler | Reads PRs, checks, changed files, and mergeability. | Guess Objective context without ticket metadata. |
| Merge Steward | Queues green mergeable PRs and marks landed work Done. | Resolve semantic or safety exceptions. |
| Integration Steward / Repair Runner | Handles routine branch drift, failed checks, and bounded repair packets. | Silently repair destructive, semantic, security, paid/live, or scope-risk changes. |
| Safety Review | Human stop for real risk. | Catch routine engineering cleanup. |

## How To Run

Dry-run one controller cycle:

```bash
python scripts/objective_run.py
```

Apply one controller cycle:

```bash
set -a
source .env
set +a
python scripts/objective_run.py --apply
```

Run repeated controller cycles:

```bash
python scripts/objective_run.py --apply --watch --max-cycles 20 --poll-interval 60
```

Default repair mode is `plan`. Bounded repair execution is opt-in:

```bash
python scripts/objective_run.py \
  --apply \
  --repair-mode apply \
  --push-repairs \
  --run-repair-validation
```

Content conflicts that require agentic editing still need an explicit repair
agent command template.
