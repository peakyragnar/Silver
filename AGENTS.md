# Silver Agent Guide

Michael owns this. Start each agent run by saying hi Michael plus one motivating
line.

This file is a map, not a manual. Keep it short. Read deeper docs only when
they are relevant to the task.

## Sources Of Truth

- Product/build contract: [`SPEC.md`](SPEC.md)
- Docs map: [`docs/index.md`](docs/index.md)
- Architecture boundaries: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- Point-in-time discipline: [`docs/PIT_DISCIPLINE.md`](docs/PIT_DISCIPLINE.md)
- Testing and validation: [`docs/TESTING.md`](docs/TESTING.md)
- Secrets and data handling: [`docs/SECURITY.md`](docs/SECURITY.md)
- Active execution plans: [`docs/exec-plans/active/`](docs/exec-plans/active/)
- Symphony setup: [`docs/SYMPHONY.md`](docs/SYMPHONY.md)

## Non-Negotiables

- Never commit `.env`, API keys, local credentials, or vendor secrets.
- No feature value may be used without an `available_at` rule.
- No prediction may be written without frozen feature/model/prompt versions.
- No backtest result may be reported without costs, baselines, and
  reproducibility metadata.
- Do not import Arrow code, schemas, or analyst-facing views. Optional Arrow raw
  caches are vendor-byte mirrors only.
- Prefer small reversible changes with explicit validation evidence.

## Work Protocol

1. Read the ticket, `SPEC.md`, and the smallest relevant docs.
2. Update the active plan or Linear workpad before editing.
3. Reproduce or define the expected behavior before implementing.
4. Keep edits scoped to the requested layer.
5. Run targeted validation first, then broader checks when available.
6. Record commands run and any skipped checks in the handoff.

## Validation Defaults

Use the strongest command that exists in the repo at the time:

```bash
git diff --check
python -m pytest
ruff check .
python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed
```

If a command is not yet available, say so plainly instead of inventing a pass.

## Git

- Work on branches off `main`.
- Keep `.env` untracked.
- Do not rewrite history or force-push unless Michael explicitly asks.
- Pull requests should follow `.github/pull_request_template.md`.
