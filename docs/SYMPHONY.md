# Symphony Setup

Silver is prepared to be run by the local Symphony checkout at
`/Users/michael/symphony`.

## Prerequisites

- The Silver repository must be pushed to GitHub so Symphony can clone it.
- `LINEAR_API_KEY` must be set in the shell that launches Symphony.
- `WORKFLOW.md` must have the correct Linear `tracker.project_slug`.
- The local Symphony Elixir dependencies should already be installed under
  `/Users/michael/symphony/elixir`.

## Configure

[`../WORKFLOW.md`](../WORKFLOW.md) is wired to the Linear project:

- Project: Silver
- URL: `https://linear.app/arrow1/project/silver-af92bd962fcf`
- Slug: `silver-af92bd962fcf`

If the Linear project changes, edit [`../WORKFLOW.md`](../WORKFLOW.md):

- Set `tracker.project_slug` to the new Silver Linear project slug.
- Keep `agent.max_concurrent_agents` low at first (`1` or `2`).
- Keep the workspace root outside this repository.

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
