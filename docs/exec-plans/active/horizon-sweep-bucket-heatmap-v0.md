# Horizon Sweep And Bucket Heatmap v0 Plan

Status: Implemented; live sweep pending

Goal: Silver can evaluate each configured hypothesis across the canonical
forward-return horizons and show the result as a readable horizon matrix plus
bucket-level stability view.

## Decision Anchor

- Goal: determine whether current hypotheses fail everywhere or only at the
  already-tested 63-trading-day horizon.
- User value: Michael can see which feature families deserve follow-up before
  adding AI-generated hypotheses.
- Constraints: preserve point-in-time labels, costs, baselines, label-scramble
  evidence, reproducible backtest metadata, and ignored generated reports.
- Evidence: current registry has 11 candidate hypotheses evaluated at 63 days;
  no persisted backtest runs exist yet for 5, 21, 126, or 252 days.
- Falsifier: if the sweep cannot explain every candidate/horizon result from
  durable `backtest_runs` and `hypothesis_evaluations`, it is not ready.

## Scope

- Add a horizon-sweep runner for `config/feature_candidates.yaml`.
- Run each candidate over canonical horizons: `5`, `21`, `63`, `126`, `252`.
- Reuse the existing falsifier, label-scramble, walk-forward window, and
  hypothesis registry paths instead of inventing a second evaluation system.
- Extend the research report with a horizon matrix and bucket heatmap summary.
- Keep the first version markdown-first; do not build an interactive dashboard
  yet.

## Design

### Candidate And Horizon Identity

A tested experiment is:

```text
feature + direction + universe + horizon + target kind
```

The candidate YAML remains the base idea registry. The sweep runner creates a
deterministic evaluated-hypothesis identity per horizon. For compatibility with
the existing 63-day rows, v0 may keep the base `hypothesis_key` for the 63-day
cell and use horizon-suffixed keys for the other cells, for example:

```text
momentum_6_1        -> existing 63-day row
momentum_6_1__h5    -> 5-day row
momentum_6_1__h21   -> 21-day row
momentum_6_1__h126  -> 126-day row
momentum_6_1__h252  -> 252-day row
```

Each horizon-suffixed row should include metadata that links it back to the base
candidate:

```json
{
  "base_hypothesis_key": "momentum_6_1",
  "feature": "momentum_6_1",
  "candidate_pack": "numeric_feature_pack_v1",
  "selection_direction": "high",
  "horizon_sweep": true
}
```

This avoids a schema migration for v0 while keeping horizon-specific evidence
separate and auditable. A later migration can normalize base candidates and
experiment variants if this structure becomes painful.

### Runner Behavior

Planned command:

```bash
python scripts/run_feature_candidate_horizon_sweep.py --universe falsifier_seed
```

Implemented behavior:

- Validate local dependencies with `--check`.
- Materialize configured candidate features once before the sweep unless
  `--skip-materialize` is passed.
- For each candidate/horizon pair, run the existing falsifier with the
  candidate's `source_feature` and `selection_direction`.
- Record the latest successful falsifier run into the hypothesis registry with
  the correct horizon-specific key.
- Default to skipping already-linked candidate/horizon cells unless
  `--rerun-existing` is passed.
- Print a compact matrix summary at the end.

### Research Report Behavior

Extend `scripts/research_results_report.py` with:

- a horizon matrix grouped by base hypothesis key
- per-cell verdict, failure reason, strategy return, baseline return, and
  difference
- a bucket heatmap using the persisted `walk_forward_windows`
- a reason glossary that still explains `label_scramble_failed`,
  `walk_forward_unstable`, and insufficient-data cases

Example matrix:

```text
hypothesis             5d       21d      63d      126d     252d
momentum_12_1          reject   reject   reject   pending  pending
revenue_growth_yoy     pending  pending  reject   pending  pending
```

Example bucket heatmap:

```text
momentum_6_1 @ 63d
2016: + -
2017: - -
2018: + -
...
```

`+` means the bucket beat the equal-weight baseline. `-` means it did not.

## Implementation Steps

1. [x] Add tests for horizon-specific hypothesis keys and report grouping.
2. [x] Add a horizon-sweep planning layer that expands candidates into
   candidate/horizon cells.
3. [x] Add `scripts/run_feature_candidate_horizon_sweep.py`.
4. [x] Reuse existing candidate materialization and falsifier execution helpers.
5. [x] Record each cell into `hypotheses` and `hypothesis_evaluations`.
6. [x] Extend `research_results_report.py` with a horizon matrix.
7. [x] Add bucket heatmap rendering from `backtest_runs.metrics.walk_forward_windows`.
8. [x] Update runbook and testing docs with the new command.

## Acceptance Criteria

- [x] `--check` validates config and canonical horizon list without live services.
- [x] Default run covers every configured candidate across all five horizons.
- [ ] Existing 63-day results are reused or skipped unless rerun is requested.
- [ ] Every completed cell links to a durable `backtest_runs` row.
- [x] The research report shows a complete candidate-by-horizon matrix.
- [x] Each completed cell can show bucket stability evidence.
- [x] No generated markdown report is committed.

## Validation

Use the strongest available checks at implementation time:

```bash
git diff --check
python scripts/run_feature_candidate_horizon_sweep.py --check
python scripts/research_results_report.py --check
python -m pytest
ruff check .
```

For live validation after `DATABASE_URL` is available:

```bash
python scripts/run_feature_candidate_horizon_sweep.py --universe falsifier_seed
python scripts/research_results_report.py
```

## Delete Or Defer

- Defer AI-generated hypotheses until the matrix shows which families and
  horizons are genuinely weak or interesting.
- Defer noncanonical horizons such as 180 trading days.
- Defer an interactive dashboard; markdown is enough for v0.
- Defer schema migration unless horizon-suffixed keys prove too awkward.

## Risks

- Long horizons have fewer scoreable late-period rows because labels need more
  future data.
- Running all cells is more expensive than the current 63-day pack, so the
  runner should support candidate and horizon filters.
- Existing 63-day rows use base hypothesis keys; report grouping must handle
  both base and horizon-suffixed keys.
