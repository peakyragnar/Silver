# Phase 1 Local Runbook

This runbook is the shortest local path to a Phase 1 momentum falsifier report.
All commands run from the repository root.

## 1. Prepare The Shell

Install the locked Python environment and use it for every command:

```bash
uv sync --locked --group dev --python 3.10
source .venv/bin/activate
```

Set the database URL in your shell. You can store it in local `.env`, but load
it into the shell before running these commands because the scripts read
environment variables directly. Do not commit `.env`.

```bash
export DATABASE_URL=postgresql://localhost:5432/silver
```

Set `FMP_API_KEY` only when you need to fetch prices from FMP. Existing local
prices, dry runs, and `--check` commands do not need it.

```bash
export FMP_API_KEY=...
```

Run the local preflight. It checks only local commands, imports, environment
variable presence, and expected repository paths; it does not connect to
Postgres or FMP and it does not print secret values.

```bash
python scripts/check_phase1_environment.py --check
```

Optional `FMP_API_KEY` warnings are expected before live price ingest. Any
`FAIL` line is a hard prerequisite to fix before starting a long run.

With `FMP_API_KEY` unset, a healthy local setup ends like this:

```text
Summary: 20 ok, 1 warning(s), 0 error(s)
Result: Phase 1 environment check passed
```

## 2. Bootstrap The Database

Create the local Postgres database if it does not already exist, then validate
and apply the Silver bootstrap sequence. Skip `createdb` if your database
already exists.

```bash
createdb silver
python scripts/bootstrap_database.py --check
python scripts/bootstrap_database.py
```

The bootstrap applies migrations, seeds `available_at` policies, seeds the
`falsifier_seed` universe, and seeds the trading calendar.

## 3. Ingest Daily Prices

Validate the FMP price-ingest config without DB or FMP access:

```bash
python scripts/ingest_fmp_prices.py --check
```

Inspect the persisted universe/date plan without calling FMP:

```bash
python scripts/ingest_fmp_prices.py --dry-run
```

Fetch and persist prices after `FMP_API_KEY` is set:

```bash
python scripts/ingest_fmp_prices.py --universe falsifier_seed
```

Use explicit `--start-date YYYY-MM-DD` and `--end-date YYYY-MM-DD` when you want
a reproducible ingest window.

## 4. Materialize Labels And Features

Forward-return labels come from normalized prices and remain unavailable until
their horizon has elapsed.

```bash
python scripts/materialize_forward_labels.py --check
python scripts/materialize_forward_labels.py --universe falsifier_seed
```

Materialize the deterministic 12-1 momentum feature from prices:

```bash
python scripts/materialize_momentum_12_1.py --check
python scripts/materialize_momentum_12_1.py --dry-run --universe falsifier_seed
python scripts/materialize_momentum_12_1.py --universe falsifier_seed
```

## 5. Run The Falsifier

Validate the falsifier CLI/config/report path first:

```bash
python scripts/run_falsifier.py --check
```

Run the Phase 1 report:

```bash
python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed
```

The default report is written to:

```text
reports/falsifier/week_1_momentum.md
```

## 6. Read The Report

Start with `Status`, `Data Coverage`, and `Failure Modes`. Then check
`Headline Metrics`, `Baseline Comparison`, `Costs Assumption`, and
`Reproducibility`.

The report is falsifier evidence, not an alpha claim. The useful outcome is a
reproducible pass, failure, or insufficient-data result with costs, baseline
comparison, and reproducibility metadata.

## Common Failure Messages

| Message | Meaning | Fix |
| --- | --- | --- |
| `psql is required` | The Postgres client is missing from `PATH`. | Install Postgres client tools and rerun the preflight. |
| `DATABASE_URL is required` | DB-backed commands do not know which database to use. | Export `DATABASE_URL` or pass `--database-url`. |
| `FMP_API_KEY is required unless --check or --dry-run is used` | Live FMP price ingest needs a vendor key. | Export `FMP_API_KEY`, or use `--check`/`--dry-run`. |
| `psycopg is required` or `Python import psycopg: unavailable` | The active Python environment is missing project dependencies. | Run `uv sync --locked --group dev --python 3.10` and activate `.venv`. |
| `If the schema is missing, run python scripts/bootstrap_database.py first` | The database is reachable but not bootstrapped for Silver. | Run the bootstrap commands in section 2. |
| `feature definition momentum_12_1 is not persisted` | Momentum feature materialization has not written its metadata yet. | Run `python scripts/materialize_momentum_12_1.py --universe falsifier_seed`. |
| `no persisted momentum_12_1 feature values exist` | Feature rows are missing for the requested universe. | Re-run momentum feature materialization after price ingest. |
| `no forward-return labels exist` | Label rows are missing for the requested horizon. | Re-run forward-label materialization after price ingest. |
| `feature values and forward-return labels do not overlap` | Features and labels cover different security/date pairs. | Re-run labels and features over the same price coverage. |
