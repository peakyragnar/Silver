---
tracker:
  kind: linear
  project_slug: "silver-af92bd962fcf"
  active_states:
    - Todo
    - In Progress
    - Rework
    - Merging
  terminal_states:
    - Done
    - Canceled
    - Duplicate
polling:
  interval_ms: 10000
workspace:
  root: ~/silver-agent-workspaces
hooks:
  after_create: |
    git clone --depth 1 https://github.com/SilverEnv/Silver.git .
    if [ -f .codex/worktree_init.sh ]; then
      bash .codex/worktree_init.sh
    fi
  before_remove: |
    true
agent:
  max_concurrent_agents: 5
  max_turns: 12
codex:
  command: codex --config shell_environment_policy.inherit=all --config 'model="gpt-5.5"' app-server
  approval_policy: never
  thread_sandbox: danger-full-access
  turn_sandbox_policy:
    type: dangerFullAccess
---

You are working on Linear ticket `{{ issue.identifier }}` for the Silver
repository.

{% if attempt %}
Continuation context:

- This is retry attempt #{{ attempt }}.
- Resume from the current workspace state.
- Do not repeat completed investigation unless new changes require it.
{% endif %}

Issue context:
Identifier: {{ issue.identifier }}
Title: {{ issue.title }}
Current status: {{ issue.state }}
Labels: {{ issue.labels }}
URL: {{ issue.url }}

Description:
{% if issue.description %}
{{ issue.description }}
{% else %}
No description provided.
{% endif %}

## Operating Rules

1. Work only inside this repository copy.
2. Start by reading `AGENTS.md`, then the smallest relevant docs.
3. Treat `SPEC.md` as the product contract.
4. Keep one persistent `## Codex Workpad` Linear comment current when Linear
   tooling is available.
5. Never commit `.env` or secrets.
6. Prefer narrow, reversible implementation with concrete validation evidence.
7. Final message must include completed actions, validation, and blockers only.

## Status Routing

- `Backlog`: do not modify; stop.
- `Todo`: move to `In Progress`, create or refresh the workpad, then execute.
- `In Progress`: continue execution from the workpad.
- `Rework`: resume from review feedback, repair the existing PR, refresh the
  proof packet, then move back to `Human Review`.
- `Human Review`: wait for human review; do not code.
- `Merging`: do not add new scope. Act as merge steward: verify the approved PR,
  update/rebase only if required, rerun failed or stale checks, add the PR to
  GitHub merge queue, confirm it lands on `main`, then move the issue to `Done`.
- `Done`: terminal; stop.
- `Canceled`: terminal; stop.
- `Duplicate`: terminal; stop.

## Execution Checklist

1. Read the issue and relevant docs.
2. Record the plan, acceptance criteria, and validation in the workpad.
3. Capture a reproduction signal or explicit expected behavior before editing.
4. Sync from `origin/main` using the repo-local pull skill when appropriate.
5. Implement only the current ticket scope.
6. Run targeted validation, then broader available validation.
7. Commit cleanly.
8. Push a branch and open/update a pull request when GitHub access is available.
9. Post a proof packet to Linear.
10. Move to `Human Review` only after acceptance criteria and validation are
    complete or a true external blocker is documented.

## Proof Packet

Before moving a ticket to `Human Review`, post a Linear comment headed
`## Proof Packet` with:

- PR link.
- Changed files summary.
- Acceptance criteria status.
- Validation commands run and outcome.
- CI status or link if available.
- Risks, assumptions, and known gaps.
- Generated artifact path or link when the ticket creates an artifact.
- Exact blocker if the work cannot be completed.

Do not move to `Human Review` with only a prose claim. The packet must give
Michael enough evidence to approve, reject, or move the issue to `Rework`.

## Merge Steward Rules

When current status is `Merging`:

1. Find the PR associated with the Linear issue identifier.
2. Confirm the PR still matches the approved scope and has passing required
   checks.
3. If GitHub requires an update, perform the smallest branch update and rerun
   validation.
4. Add the PR to GitHub merge queue; do not bypass the queue.
5. After merge, pull/verify `origin/main` contains the merge commit when
   practical.
6. Move the Linear issue to `Done` and post a short merge confirmation.

## Silver-Specific Quality Bar

- Point-in-time correctness is mandatory.
- Backtest claims require costs, baselines, and reproducibility metadata.
- Labels must not be available before their horizon elapses.
- Feature/model/prompt versions must be immutable once referenced by a run.
- Generated reports must state the exact commands and metadata used.

## Validation Defaults

Run whichever of these exist for the current repository state:

```bash
git diff --check
python -m pytest
ruff check .
python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed
```

If a validation command does not exist yet, record that plainly in the workpad
and final handoff.
