# Symphony Setup

Silver is prepared to be run by the local Symphony checkout at
`/Users/michael/symphony`.

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
- Keep `Human Review` as a non-active Linear state so completed PRs stop being
  picked up by Symphony while they wait for Michael's review.
- Keep `Merging` as a non-active Linear state. It is handled by the lightweight
  merge steward script instead of a full Codex worker.

## Linear State Machine

Silver uses Linear as the Symphony control plane:

| State | Symphony active? | Meaning |
|---|---:|---|
| `Backlog` | No | Planned work; do not start. |
| `Todo` | Yes | Ready for an agent to start. |
| `In Progress` | Yes | Agent is implementing the ticket. |
| `Rework` | Yes | Human or CI requested changes; agent should repair the PR. |
| `Human Review` | No | Agent has posted a proof packet and is waiting for Michael. |
| `Merging` | No | Michael approved; lightweight merge steward queues/marks done. |
| `Done` | No | Complete. |
| `Canceled` / `Duplicate` | No | Terminal non-work states. |

The key rule is that agents do not mark their own implementation work `Done`.
They move implementation tickets to `Human Review` with evidence. Michael moves
approved tickets to `Merging`; the lightweight merge steward handles GitHub
merge queue and marks the issue `Done` after the PR lands. Only conflicts or
failed checks move back to `Rework` for Codex.

## Proof Packets

Every ticket moved to `Human Review` should have a Linear comment headed
`## Proof Packet` containing:

- PR link.
- Changed files summary.
- Acceptance criteria status.
- Validation commands run and outcome.
- CI status or link.
- Risks, assumptions, and known gaps.
- Generated artifact path or link when relevant.

For Silver, proof is usually a command result, test output, migration check, or
generated report. A prose claim that something is done is not enough.

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

## Merge Steward

Run the merge steward in the Silver repository shell where `LINEAR_API_KEY` is
available. It reads Linear issues in `Merging`, finds the matching GitHub PR,
queues green PRs, marks merged PRs `Done`, and sends conflicts or failed checks
to `Rework`.

Validate local wiring without network writes:

```bash
python scripts/merge_steward.py --check
```

Preview current actions:

```bash
python scripts/merge_steward.py --dry-run
```

Keep it running while reviewing batches:

```bash
tmux kill-session -t silver-merge-steward 2>/dev/null || true
tmux new-session -d -s silver-merge-steward '
  set -a
  source /Users/michael/Silver/.env
  set +a
  cd /Users/michael/Silver
  uv run python scripts/merge_steward.py --watch --poll-interval 30
'
```

The steward is deterministic on purpose. It does not edit code. It only queues,
waits, marks `Done`, or routes exceptions to `Rework`.

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
