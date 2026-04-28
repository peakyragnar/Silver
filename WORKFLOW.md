---
tracker:
  kind: linear
  project_slug: "silver-af92bd962fcf"
  active_states:
    - Todo
    - In Progress
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
    git clone --depth 1 https://github.com/peakyragnar/Silver.git .
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
- `In Review`: wait for human review; do not code.
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
9. Move to `In Review` only after acceptance criteria and validation are
   complete or a true external blocker is documented.

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
python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 30 --universe falsifier_seed
```

If a validation command does not exist yet, record that plainly in the workpad
and final handoff.
