# Architecture

Silver is a layered point-in-time research system. The target structure is in
`SPEC.md`; this file turns it into working rules for agents.

## Layer Order

Code should move data in this order:

1. ingest clients and raw vault
2. point-in-time normalization
3. feature store
4. labels
5. models and walk-forward backtests
6. hypotheses
7. portfolio simulation and paper trading
8. runtime jobs and reports

Avoid reverse dependencies. For example, ingestion code must not depend on
backtest code, and feature code must not read prediction outcomes.

## Expected Repository Shape

```text
config/
db/migrations/
db/seed/
src/silver/
scripts/
tests/
reports/
```

Create only the directories needed for the current phase. Do not scaffold large
empty systems just to mirror the final spec.

## Database Boundary

All application tables live in the `silver` Postgres schema. Migrations are the
source of truth for table shape. Python code should use typed helper functions
or repository-local query helpers rather than ad hoc SQL scattered across
scripts.

Numbered SQL migrations live under `db/migrations/`. Run
`python scripts/apply_migrations.py --check` to validate migration order and
static schema expectations without a live database. To apply migrations to a
clean local Postgres database, set `DATABASE_URL` and run
`python scripts/apply_migrations.py`; the apply path records checksums in
`silver.schema_migrations` and requires the `psql` client.

## External Data Boundary

Allowed sources for v1 are FMP, SEC EDGAR, optional Arrow raw byte caches, and
optional Norgate data later for delistings. Do not depend on Arrow normalized
tables, Arrow Python code, or analyst-facing views.

## LLM Boundary

LLMs may extract structured text features, propose hypotheses, and explain
validated results. They must not compute returns, write labels, write
predictions, score outcomes, override risk controls, or validate their own
ideas.
