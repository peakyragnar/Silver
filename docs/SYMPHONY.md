# Symphony Setup

Silver is prepared to be run by the local Symphony checkout at
`/Users/michael/symphony`.

This file is the local setup and runbook. For Silver's operating policy on top
of Symphony, including Objectives, ticket flow, migration coordination, and
conflict handling, see [`Symphony-Operation.md`](Symphony-Operation.md).

## Prerequisites

- The Silver repository must be pushed to GitHub so Symphony can clone it.
- `LINEAR_API_KEY` must be set in the shell that launches Symphony.
- `WORKFLOW.md` must have the correct Linear `tracker.project_slug`.
- Codex CLI `0.125.0` or newer is required for the pinned `gpt-5.5` model.
- The workflow uses `danger-full-access` for Codex turns so unattended agents can
  create branches, commits, and PR handoffs. Only move trusted, scoped tickets
  into active states.
- The local Symphony Elixir dependencies should already be installed under
  `/Users/michael/symphony/elixir`.

## Configure

[`../WORKFLOW.md`](../WORKFLOW.md) is wired to the Linear project:

- Project: Silver
- URL: `https://linear.app/arrow1/project/silver-af92bd962fcf`
- Slug: `silver-af92bd962fcf`

If the Linear project changes, edit [`../WORKFLOW.md`](../WORKFLOW.md):

- Set `tracker.project_slug` to the new Silver Linear project slug.
- Keep `agent.max_concurrent_agents` at a level Michael can review; current
  operating target is `5`.
- Keep the workspace root outside this repository.
- Keep `Safety Review` as a non-active Linear state for catastrophic or
  semantic exceptions that require Michael.
- Keep `Merging` as a non-active Linear state. It is handled by the lightweight
  merge steward script instead of a full Codex worker.

## Linear Bridge

Silver currently uses Linear as Symphony's visible control bridge. That means
Linear states still decide which tickets Symphony starts today, but Linear is
not the long-term runtime database for Silver automation. The target source of
truth is a local Silver work ledger, with Linear kept only as an optional mirror
or board.

| State | Symphony active? | Meaning |
|---|---:|---|
| `Backlog` | No | Planned work; do not start. |
| `Todo` | Yes | Ready for an agent to start. |
| `In Progress` | Yes | Agent is implementing the ticket. |
| `Rework` | Yes | CI, steward, or mechanical conflict repair requested; agent should repair the PR. |
| `Safety Review` | No | Serious safety or semantic exception requiring Michael. |
| `Merging` | No | Safe completed work; lightweight merge steward queues/marks done. |
| `Done` | No | Complete. |
| `Canceled` / `Duplicate` | No | Terminal non-work states. |

The key rule is that agents do not mark their own implementation work `Done`.
Safe completed tickets post proof-packet evidence and move to `Merging`. The
lightweight merge steward handles GitHub merge queue and marks the issue `Done`
after the PR lands. Failed checks and mechanical conflicts move back to
`Rework`; destructive, semantic, paid/live, security, or scope-drift exceptions
move to `Safety Review`.

Do not use Linear as a high-frequency polling database. Stewards should read
Linear in bounded calls, update it only on state changes, and back off on rate
limits. If Linear is unavailable, active GitHub PRs and the future Silver work
ledger should remain the operational source of truth.

## Proof Packets

Every safe completed ticket moved to `Merging` should have a Linear comment
headed `## Proof Packet` containing:

- PR link.
- Parent Objective, when the ticket belongs to one.
- Ledger ticket ID.
- Ticket Role: `contract`, `implementation`, `integration`, `validation`, or
  `docs`.
- Dependency Group and Contracts Touched.
- Risk Class.
- Objective Impact: 1-2 user-facing sentences explaining how this ticket moves
  the parent Objective forward.
- Changed files summary.
- Acceptance criteria status.
- Validation commands run and outcome.
- CI status or link.
- Risks, assumptions, and known gaps.
- Generated artifact path or link when relevant.
- Exact blocker when routed to `Safety Review`.

For Silver, proof is usually a command result, test output, migration check, or
generated report. A prose claim that something is done is not enough. Proof
packets are audit receipts and steward inputs, not routine approval requests.

## Work Ledger

Silver now has a local work-ledger MVP for fast Objective and ticket state:

```bash
python scripts/work_ledger.py init
python scripts/work_ledger.py import-objectives
python scripts/work_ledger.py status
python scripts/work_ledger.py admit --max-active 5 --ready-buffer 5
python scripts/work_ledger.py list-runnable
```

Preview the Linear mirror actions that would make local ledger state visible to
Symphony:

```bash
python scripts/linear_mirror.py
```

Apply those visible-board changes only when the preview is correct:

```bash
python scripts/linear_mirror.py --apply
```

After Symphony opens or lands PRs, reconcile GitHub state back into the local
ledger before mirroring again:

```bash
python scripts/vcs_reconciler.py
python scripts/vcs_reconciler.py --apply
python scripts/integration_steward.py
python scripts/integration_steward.py --apply
python scripts/integration_repair_runner.py
python scripts/linear_mirror.py
python scripts/linear_mirror.py --apply
```

The VCS reconciler matches PRs by `Ledger Ticket: ...`, `ticket_id: ...`,
Linear identifier, or ledger ticket ID in PR identity fields. It records PR URL
and branch evidence, marks merged PRs `Done`, moves safe green open PRs to
`Merging`, routes failed checks or conflicts to `Rework`, and routes scope or
safety exceptions to `Safety Review`. It does not repair code yet.

The integration steward turns `Rework` into a repair packet. The repair runner
can then prepare an isolated worktree for the PR branch, merge current `main`,
run an optional agent command, run validation, push the repaired branch, and
move the ticket back to `Merging`.

The repair runner is dry-run by default:

```bash
python scripts/integration_repair_runner.py
```

To execute only bounded branch repair:

```bash
python scripts/integration_repair_runner.py --apply --push --run-validation
```

Content conflicts require an explicit agent command template:

```bash
python scripts/integration_repair_runner.py \
  --apply --push --run-validation \
  --agent-command 'repair-agent --packet {packet_file} --worktree {worktree}'
```

The runner does not force-push, does not rewrite history, and does not resolve
semantic or safety exceptions silently.

By default the ledger lives at:

```bash
/Users/michael/Silver/.silver/work_ledger.db
```

Set `SILVER_LEDGER_PATH` when stewards or workers need a shared path outside an
isolated workspace. The ledger is local runtime state and must not be committed.

For now, Symphony still starts workers from the Linear bridge. The ledger is the
next control-plane layer: stewards should move to reading the ledger first, then
mirror visible state to Linear only on changes. The mirror maps local `Ready`
tickets to Linear `Todo`, so Symphony can keep working while the local ledger
becomes the source of truth.

## Run

The current local run is managed by `tmux` session `silver-symphony` so it can
stay alive independently of a shell.

Start or restart it with:

```bash
tmux kill-session -t silver-symphony 2>/dev/null || true
tmux new-session -d -s silver-symphony '
  set -a
  source /Users/michael/Silver/.env
  set +a
  cd /Users/michael/symphony/elixir
  mise exec -- ./bin/symphony /Users/michael/Silver/WORKFLOW.md \
    --logs-root /Users/michael/Silver/.symphony/log \
    --port 4007 \
    --i-understand-that-this-will-be-running-without-the-usual-guardrails
'
```

The dashboard is available at `http://localhost:4007` when `--port` is set.

Inspect:

```bash
tmux capture-pane -pt silver-symphony -S -80
curl -fsS http://127.0.0.1:4007/api/v1/state
```

Stop:

```bash
tmux kill-session -t silver-symphony
```

## Admission Steward

Run the admission steward in the Silver repository shell where
`LINEAR_API_KEY` is available. It reads approved Objective files, Backlog
tickets, active Symphony states, blocking relations, and open GitHub PRs, then
promotes safe runnable work from `Backlog` to `Todo`.

Validate local wiring without network writes:

```bash
python scripts/admission_steward.py --check
```

Preview current admission decisions:

```bash
python scripts/admission_steward.py --dry-run --max-active 5 --todo-buffer 5
```

Promote once:

```bash
python scripts/admission_steward.py --promote --max-active 5 --todo-buffer 5
```

Keep it running while approved Objectives should feed Symphony:

```bash
tmux kill-session -t silver-admission-steward 2>/dev/null || true
tmux new-session -d -s silver-admission-steward '
  set -a
  source /Users/michael/Silver/.env
  set +a
  cd /Users/michael/Silver
  uv run python scripts/admission_steward.py --watch --promote \
    --max-active 5 --todo-buffer 5 --poll-interval 300
'
```

The steward does not build, review, merge, or resolve conflicts. It only admits
approved, runnable Backlog tickets into `Todo` and records why each candidate
was promoted, skipped, or left waiting.

## Merge Steward

Run the merge steward in the Silver repository shell where `LINEAR_API_KEY` is
available. It reads Linear issues in `Merging`, reconciles stale nonterminal
project issues whose matching GitHub PR is already merged, classifies Safety
Review risks from issue text, PR metadata, changed files, and available diffs,
queues safe green PRs, marks merged PRs `Done`, sends conflicts or failed checks
to `Rework`, and sends destructive, semantic, paid/live, security, scope-drift,
or automation-permission exceptions to `Safety Review` with a blocker comment.
The Objective-aware VCS reconciler may allow a narrower case before Linear is
updated: additive docs-only PIT clarifications made by a `contract` ticket
inside its declared `Owns` paths.

Validate local wiring without network writes:

```bash
python scripts/merge_steward.py --check
```

Preview current actions:

```bash
python scripts/merge_steward.py --dry-run
```

Dry-run stale reconciliation lines use the `stale_mark_done` action and perform
no Linear writes.

Keep it running while batches run:

```bash
tmux kill-session -t silver-merge-steward 2>/dev/null || true
tmux new-session -d -s silver-merge-steward '
  set -a
  source /Users/michael/Silver/.env
  set +a
  cd /Users/michael/Silver
  uv run python scripts/merge_steward.py --watch --poll-interval 300
'
```

The steward is deterministic on purpose. It does not edit code. It only queues,
waits, marks `Done`, or routes exceptions to `Rework` or `Safety Review`.

## First Tickets

Use small, independently reviewable Linear tickets first:

1. Bootstrap Python project and validation tooling
2. Add foundation database migration
3. Seed trading calendar and seed securities
4. Implement daily price ingest into raw vault
5. Compute forward labels
6. Run the first 12-1 momentum falsifier

Do not start high-concurrency runs until the first two or three tickets produce
clean PRs and useful workpad notes.
