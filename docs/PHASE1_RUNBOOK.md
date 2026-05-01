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

Before a live Phase 1 run, add the live database readiness check. This executes
a sanitized `SELECT 1` against `DATABASE_URL` and still hides connection
details:

```bash
python scripts/check_phase1_environment.py --check --live-db
```

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
python scripts/check_phase1_environment.py --check --live-db
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

FMP ingest must raw-vault every HTTP response returned by the transport before
parsing, retrying, returning success, or raising for a terminal non-2xx status.
The persisted request URL and params must be redacted, and metadata must record
attempt number, retry budget, retryable/terminal status, and outcome. The
current raw vault stores one row per unique `(vendor, endpoint, params_hash,
raw_hash)`; byte-identical retry bodies can dedupe to an existing row even
though the client still must attempt a write for each response.

## 4. Materialize Labels And Features

Forward-return labels come from normalized prices and remain unavailable until
their horizon has elapsed.

```bash
python scripts/materialize_forward_labels.py --check
python scripts/materialize_forward_labels.py --universe falsifier_seed
```

Raw-return labels are the default. To also populate benchmark-relative fields,
provide a benchmark ticker whose daily prices already exist in `prices_daily`;
the benchmark is loaded explicitly and is not added to the prediction universe.

```bash
python scripts/materialize_forward_labels.py --universe falsifier_seed --benchmark-ticker SPY
```

Materialize the deterministic 12-1 momentum feature from prices:

```bash
python scripts/materialize_momentum_12_1.py --check
python scripts/materialize_momentum_12_1.py --dry-run --universe falsifier_seed
python scripts/materialize_momentum_12_1.py --universe falsifier_seed
```

Feature Candidate Pack v1 materializes the configured set of deterministic
numeric candidates in `config/feature_candidates.yaml` for multi-hypothesis
evaluation:

- `momentum_12_1`, where high values are selected
- `avg_dollar_volume_63`, where high values are selected
- `momentum_6_1`, where high values are selected
- `momentum_3_0`, which uses the persisted `return_63_0` feature and selects
  high values
- `short_reversal_21_0`, which uses the persisted `return_21_0` feature and
  tells the falsifier to select low values
- `low_realized_volatility_63`, which uses the persisted
  `realized_volatility_63` feature and tells the falsifier to select low values

```bash
python scripts/materialize_feature_candidates.py --check
python scripts/materialize_feature_candidates.py --dry-run --universe falsifier_seed
python scripts/materialize_feature_candidates.py --universe falsifier_seed
```

Use `--candidate-config <path>` to review or run a different YAML candidate
pack without changing Python runner code. The YAML owns candidate identity,
thesis text, source feature, ranking direction, and materializer name; Python
still owns the allowed materializers and point-in-time calculations.

## 5. Run The Falsifier

Validate the falsifier CLI/config/report path first:

```bash
python scripts/run_falsifier.py --check
```

Run the Phase 1 report:

```bash
python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed
```

The same falsifier can rank any persisted numeric feature. For candidates where
lower feature values are the hypothesis, pass `--selection-direction low`; the
stored feature value remains raw, and the backtest metadata records the
direction used for ranking.

```bash
python scripts/run_falsifier.py --strategy avg_dollar_volume_63 --horizon 63 --universe falsifier_seed --output-path reports/falsifier/candidate_pack/avg_dollar_volume_63_h63.md
python scripts/run_falsifier.py --strategy realized_volatility_63 --selection-direction low --horizon 63 --universe falsifier_seed --output-path reports/falsifier/candidate_pack/low_realized_volatility_63_h63.md
```

The default report is written to:

```text
reports/falsifier/week_1_momentum.md
```

To replay a persisted successful falsifier claim from the durable backtest
identity, first inspect the stored replay contract without rerunning:

```bash
python scripts/run_falsifier.py --replay-backtest-run-id 2 --replay-dry-run
```

Healthy dry-run output names the stored identity, the reconstructed run command,
and the frozen replay inputs:

```text
OK: falsifier replay dry-run loaded accepted claim metadata
Replay identity: model_run_id=1; model_run_key=model-run-1; backtest_run_id=2; backtest_run_key=backtest-run-1
Replay command: python scripts/run_falsifier.py --replay-backtest-run-id 2
Resolved run command: python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed
Evidence: stored accepted-claim metadata matched replay contract
Evidence: rerun not executed (--replay-dry-run)
Replay inputs: strategy=momentum_12_1; universe=falsifier_seed; horizon=63; target_kind=excess_return_market; feature_set_hash=...; joined_feature_label_rows_sha256=...; available_at_policy_versions={"daily_price":1}
```

Then rerun and compare the replayed metadata against the stored claim:

```bash
python scripts/run_falsifier.py --replay-backtest-run-id 2 --output-path reports/falsifier/week_1_momentum_replay.md
```

Successful replay prints explicit match evidence. Any mismatch exits non-zero
and names the drifted field, such as `backtest_runs.metrics`. Use
`--replay-backtest-run-key <backtest_run_key>` when the key is easier to copy
than the numeric id. A `model_run_id` alone is not accepted for full replay
because one model run can support multiple backtest claims.

## 6. Register The Hypothesis

After a falsifier run and replay pass, record the testable idea in the
hypothesis registry and link it to the durable backtest evidence. Validate the
CLI first:

```bash
python scripts/manage_hypotheses.py check
```

Seed the Phase 1 momentum hypothesis:

```bash
python scripts/manage_hypotheses.py seed-momentum
```

Then link the latest successful falsifier run for the same strategy, universe,
and horizon:

```bash
python scripts/manage_hypotheses.py record-latest-falsifier --notes "replay matched"
```

You can also link an explicit durable backtest identity:

```bash
python scripts/manage_hypotheses.py record-backtest --backtest-run-id 3
```

Inspect the current registry:

```bash
python scripts/manage_hypotheses.py list
```

To run Feature Candidate Pack v1 as one multi-hypothesis evaluation, use:

```bash
python scripts/run_feature_candidate_pack.py --check
python scripts/run_feature_candidate_pack.py --universe falsifier_seed --horizon 63
```

The pack refreshes candidate feature values by default, upserts each hypothesis,
runs the falsifier for each candidate, records the linked durable backtest
evaluation, and prints a small scoreboard. Per-candidate reports are written
under:

```text
reports/falsifier/candidate_pack/
```

Use `--skip-materialize` only when you intentionally want to evaluate the
feature values already in the database.

Use `--candidate <hypothesis_key>` to run one configured candidate, repeat it
to run several, or omit it to run the full configured pack.

To run the harder walk-forward rollup for the configured pack, use:

```bash
python scripts/run_feature_candidate_walk_forward.py --check
python scripts/run_feature_candidate_walk_forward.py --universe falsifier_seed --horizon 63
```

This runner refreshes candidates unless `--skip-materialize` is passed, runs
the same replayable falsifier per candidate, reads the persisted
`walk_forward_windows` from `backtest_runs.metrics`, and prints a consistency
scoreboard. A candidate must have enough scored windows, beat the equal-weight
baseline in a sufficient share of windows, and still pass label scramble before
the registry can treat it as promising.

This registry is navigation memory. The authoritative evidence remains the
linked `backtest_runs` and `model_runs` metadata plus replay proof.
Recording an evaluation moves the hypothesis status to `promising`,
`rejected`, `accepted`, or `running`; failed backtest execution does not by
itself retire a hypothesis.

## 7. Read The Report

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
| `database connectivity` fails | `DATABASE_URL` is present but Postgres rejected or could not reach it. | Fix the database URL, user/role, password, host, or database before running live pipeline commands. |
| `If the schema is missing, run python scripts/bootstrap_database.py first` | The database is reachable but not bootstrapped for Silver. | Run the bootstrap commands in section 2. |
| `feature definition ... is not persisted` | The requested feature materialization has not written its metadata yet. | Run `python scripts/materialize_feature_candidates.py --universe falsifier_seed`, or run the specific feature materializer. |
| `no persisted ... feature values exist` | Feature rows are missing for the requested universe. | Re-run feature candidate materialization after price ingest. |
| `no forward-return labels exist` | Label rows are missing for the requested horizon. | Re-run forward-label materialization after price ingest. |
| `feature values and forward-return labels do not overlap` | Features and labels cover different security/date pairs. | Re-run labels and features over the same price coverage. |
