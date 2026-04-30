# Objective Template

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
Tickets are compiled output from this Objective. Each ticket should include the
portable metadata below.

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
