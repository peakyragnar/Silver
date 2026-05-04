# Promising Candidate Review v0

This guide defines how to review promising research cells before adding new
hypotheses or promoting anything to accepted evidence.

Silver is a falsification machine. A promising result is not an alpha claim. It
is a candidate that survived the first gates and deserves a focused review.

## Decision Anchor

Goal: decide whether each promising candidate should be reviewed more deeply,
watched, or demoted.

User value: Michael can see which research paths deserve the next hour of work
before the system generates more experiments.

Constraints:

- Do not change hypothesis status during this review.
- Do not add AI-generated hypotheses until the current promising set is
  triaged.
- Do not treat generated markdown reports as the source of truth.
- Keep durable `backtest_runs`, `model_runs`, and linked hypothesis evaluations
  as the authoritative evidence.

Falsifier: if a promising cell is driven by one period, barely passes label
scramble, has weak adjacent horizons, or disappears after costs, it should not
advance.

## Terms

| Term | Meaning |
| --- | --- |
| Candidate | A base research idea from `config/feature_candidates.yaml`, such as `momentum_12_1`. |
| Feature | The value known at as-of date `T`, such as `momentum_12_1` or `gross_margin`. |
| Horizon | The forward-return measurement window after `T`. `h126` means 126 trading sessions after `T`. |
| Cell | One candidate tested at one horizon, such as `momentum_12_1__h126`. |
| Promising | A cell whose backtest succeeded, label scramble passed, mean edge versus baseline was positive, and walk-forward stability passed. |
| Accepted | A stronger future status for evidence that survives explicit promotion rules and adversarial review. Promising is not accepted. |
| Rejected | A cell that failed a required gate, such as baseline comparison, label scramble, or walk-forward stability. |
| Baseline | The comparison portfolio or strategy the candidate must beat after costs. Current reports use the equal-weight baseline. |
| Edge | Strategy return minus baseline return, net of costs. Positive edge means the strategy beat the baseline. |
| Walk-forward bucket | A non-overlapping test block used to check whether results repeat across time instead of only in one historical stretch. |
| Positive bucket | A bucket where the candidate beat the baseline. |
| Bucket concentration | A warning sign where most positive buckets are clustered in one short period instead of spread across years. |
| Adjacent horizons | Nearby canonical horizons for the same candidate. For `h126`, inspect `h63` and `h252`; for `h252`, inspect `h126`. |
| Label scramble | A falsification test that randomly reassigns labels. Passing means the observed result was stronger than the scrambled alternatives by the configured threshold. |
| Cost sensitivity | Whether the candidate still looks useful if trading costs or turnover assumptions become less favorable. |
| Deep dive | Recommendation to inspect replay evidence, bucket drivers, costs, and possible promotion criteria next. |
| Watch | Recommendation to keep the cell visible but not spend deep work on it yet. |
| Demote | Recommendation to stop treating the cell as promising for planning purposes unless new evidence changes. |

## Current Promising Gate

The current horizon-sweep runner treats a cell as promising only after the
formal backtest gates pass.

| Gate | Current rule |
| --- | --- |
| Backtest status | The falsifier run must finish with `succeeded`. |
| Label scramble | `label_scramble_pass` must be `true`. |
| Scored buckets | At least 2 walk-forward buckets must be scored. |
| Positive bucket rate | At least 60% of scored buckets must beat the baseline. |
| Mean edge | Mean net difference versus baseline must be greater than 0. |
| Explicit failure | No explicit rejection reason may be present. |

This is a first-pass survival test. It does not measure whether the edge is
large enough to matter, whether the result is economically useful, or whether
the candidate should be accepted.

## Current Review Set

After the first live 55-cell horizon sweep, the promising set is:

| Cell | Family | Feature | Horizon | Direction |
| --- | --- | --- | ---: | --- |
| `momentum_12_1__h126` | price | `momentum_12_1` | 126 | high |
| `avg_dollar_volume_63__h252` | price | `avg_dollar_volume_63` | 252 | high |
| `momentum_3_0__h252` | price | `return_63_0` | 252 | high |
| `gross_margin__h252` | fundamentals | `gross_margin` | 252 | high |

Do not hard-code this set into future logic. It is the current operator review
set from the latest generated cockpit.

## Operating Loop

Operate manually first. The generated review makes the evidence easier to read,
but the decision criteria should stay understandable before they are further
automated.

### 1. Refresh The Cockpit

Run from the repo root with `DATABASE_URL` set:

```bash
python scripts/research_results_report.py --check
python scripts/research_results_report.py
```

Open:

```text
reports/research/results_v0.md
```

The report is generated and git-ignored. Use it as a navigation layer only.

### 2. Find Promising Cells

Start with the `By Verdict`, `Horizon Matrix`, and `Results` sections.

A cell belongs in this review only when the report verdict is `promising`.
Rejected cells can be noted, but they do not enter the promising review unless
Michael explicitly asks to challenge the rejection rule.

### 3. Review Each Promising Cell

For each promising cell, answer:

| Question | How to read it |
| --- | --- |
| Is the edge meaningful? | Compare strategy, baseline, and difference in the `Results` table. |
| Is the evidence broad? | Check positive bucket count and heatmap spread across years. |
| Are adjacent horizons supportive? | Check the same candidate at nearby horizons in the horizon matrix. |
| Did label scramble pass comfortably? | Inspect the linked backtest report or DB-backed label-scramble metrics. |
| Could costs erase it? | Check turnover/cost assumptions in the falsifier report and flag fragile edges. |
| Is the sample credible? | Check scored buckets and whether missing data or late-start features reduce coverage. |

### 4. Assign A Recommendation

Use the weakest relevant concern. A cell with one serious fragility should not
receive `deep_dive` just because another metric looks good.

| Recommendation | Definition |
| --- | --- |
| `deep_dive` | The cell has meaningful positive edge, passed label scramble, has broad bucket support, and adjacent horizons are not contradictory. |
| `watch` | The cell passed the formal promising gates but has small edge, weak adjacent horizons, clustered buckets, or unclear cost sensitivity. |
| `demote` | The cell technically passed but the review finds evidence too fragile to guide next research work. |

Default to `watch` when evidence is mixed. Default to `demote` when the only
reason to continue is that the label says `promising`.

### 5. Read The Generated Summary And Review

The research cockpit includes two promising-candidate sections derived from
the same latest linked evidence. Both are read-only navigation, not database
status updates.

Use the narrow `Promising Candidate Summary` first when reading in a terminal.
Use the wider `Promising Candidate Review` table when you need every column on
one row.

Summary row shape:

```text
1. Cell
   recommendation: deep_dive|watch|demote
   horizon: N trading sessions
   edge: signed strategy-minus-baseline return
   buckets: positive/total bucket count
   label scramble: pass/fail plus p-value when available
   cost sensitivity: low|medium|high|unknown
   reason: recommendation reason
```

Minimum row shape:

```text
Cell | Horizon | Positive buckets | Edge | Adjacent horizons | Label scramble | Cost sensitivity | Recommendation | Reason
```

Example:

```text
momentum_12_1__h126 | 126 | 22/36 | +0.2648% | h63 rejected, h252 rejected | pass | unknown | watch | passed gates but adjacent horizons are weak
```

Use the generated recommendation as a starting point. Michael or an agent may
override it in work notes after reading the linked backtest evidence.

## How The Generated Review Works

The generated review is produced by:

```bash
python scripts/research_results_report.py
```

That command reads the latest linked hypothesis evaluations from Postgres and
writes:

```text
reports/research/results_v0.md
```

The report file is git-ignored. It is a cockpit view, not durable evidence.
The durable evidence remains in:

- `silver.hypotheses`
- `silver.hypothesis_evaluations`
- `silver.backtest_runs`
- `silver.model_runs`

The review does not write to the database. It only derives a recommendation
from the latest stored rows.

### Input Rows

For each hypothesis/cell, the report loader reads:

| Input | Used for |
| --- | --- |
| `hypothesis_key` | The displayed cell name, such as `avg_dollar_volume_63__h252`. |
| `base_hypothesis_key` | Grouping related horizon cells under the same candidate. |
| `horizon_days` | The tested forward-return horizon. |
| `evaluation_status` and `failure_reason` | The stored registry verdict and explicit rejection reason. |
| `backtest_runs.status` | Whether the falsifier run succeeded, failed, or had insufficient data. |
| `backtest_runs.metrics` | Strategy return, scored dates, and walk-forward bucket windows. |
| `backtest_runs.baseline_metrics` | Equal-weight baseline return and strategy-vs-baseline edge. |
| `backtest_runs.label_scramble_metrics` | Label-scramble p-value and alpha threshold. |
| `backtest_runs.label_scramble_pass` | Whether label scramble passed. |
| `backtest_runs.cost_assumptions` | Current round-trip trading cost assumptions. |

### Derived Summary And Table Columns

The `Promising Candidate Summary` block and `Promising Candidate Review` table
only include rows whose derived report verdict is `promising`.

| Column | How it is computed |
| --- | --- |
| `Cell` | The full hypothesis key for this candidate/horizon cell. |
| `Horizon` | The forward-return horizon in trading sessions. |
| `Positive buckets` | Count of walk-forward buckets where strategy return beat baseline return. |
| `Edge` | Mean strategy net return minus equal-weight baseline net return. |
| `Adjacent horizons` | Nearby canonical horizons for the same base candidate. |
| `Label scramble` | Pass/fail plus stored p-value and alpha when available. |
| `Cost sensitivity` | Edge divided by current round-trip cost. |
| `Recommendation` | A read-only operator suggestion: `deep_dive`, `watch`, or `demote`. |
| `Reason` | The main reason for that suggestion. |

### Adjacent Horizons

Adjacent horizons are checked to avoid trusting a one-horizon spike.

Canonical horizons are:

```text
5, 21, 63, 126, 252
```

Examples:

| Cell horizon | Adjacent horizons checked |
| ---: | --- |
| 5 | 21 |
| 21 | 5 and 63 |
| 63 | 21 and 126 |
| 126 | 63 and 252 |
| 252 | 126 |

An adjacent horizon can be rejected and still contain useful information. For
example, `avg_dollar_volume_63__h126` was rejected for walk-forward instability,
but its mean edge was still positive. That is weaker than a supportive
`promising` adjacent cell, but it is not the same as an adjacent horizon with a
negative edge.

### Cost Sensitivity

Cost sensitivity compares the observed edge to the current round-trip cost:

```text
cost multiple = edge / (round_trip_cost_bps / 10000)
```

Interpretation:

| Cost sensitivity | Meaning |
| --- | --- |
| `low` | Edge is at least 5x the current round-trip cost. Costs are less likely to erase the result. |
| `medium` | Edge is between 1x and 5x the current round-trip cost. Costs matter. |
| `high` | Edge is below 1x the current round-trip cost. Costs can erase the result. |
| `unknown` | Cost assumptions or edge were missing from the report row. |

This is a first-pass pressure test. It is not a full transaction-cost stress
test.

### Recommendation Logic

The current recommendation logic is intentionally conservative.

`demote` if:

- edge is missing
- edge is smaller than one current round-trip cost
- bucket win rate falls below the current 60% promising gate

`deep_dive` if:

- edge is at least 1.00%
- cost sensitivity is low or unknown
- adjacent horizons are not directly contradictory

`watch` if:

- the cell passed the promising gates but has a small edge
- bucket win rate is close to the 60% gate
- adjacent horizons are weak or mixed
- label-scramble p-value is missing
- cost sensitivity is unknown

The rule should be treated as navigation. It is not a promotion rule.

## How To Read It In The Terminal

Start with the narrow summary:

```bash
rg -n "Promising Candidate Summary" reports/research/results_v0.md -A 40
```

The detailed markdown table is wide. `rg -C` is good for finding it, but not
for reading it.

Use:

```bash
less -S reports/research/results_v0.md
```

Then search:

```text
/Promising Candidate Review
```

Use the left and right arrow keys to scroll horizontally.

The terminal-friendly summary is generated above the wide table:

```text
1. avg_dollar_volume_63__h252
   recommendation: deep_dive
   edge: +1.6025%
   buckets: 21/35
   reason: large edge with usable cost cushion
```

The summary is generated from the same data as the table and does not change
any recommendation logic.

## First Deep Dive

The first current `deep_dive` is:

```text
avg_dollar_volume_63__h252
```

It deserves the first detailed inspection because:

- it is the only current `deep_dive`
- its edge is materially larger than the other promising cells
- its edge has a larger cost cushion
- the adjacent 126-session horizon had positive edge even though it failed the
  walk-forward stability gate

The deep-dive report should answer:

| Question | Why it matters |
| --- | --- |
| Which years drove the edge? | Detect whether the result is broad or dominated by one regime. |
| Which buckets failed? | Identify periods where the signal breaks. |
| Which tickers were selected most often? | Detect concentration and mega-cap/liquidity bias. |
| Did it mostly select the largest or most liquid names? | Determine whether the result is really a size/liquidity exposure. |
| Why did 252 sessions pass while 126 sessions failed? | Understand whether the signal only works at long holding periods. |
| How sensitive is it to higher costs? | Check whether the edge survives realistic friction. |
| Does it overlap with momentum? | Determine whether this is a distinct signal or a proxy for another one. |

The deep dive should still be read-only. It should not promote
`avg_dollar_volume_63__h252` to `accepted`.

## Promotion Boundary

Promising is a triage label. It should not automatically become accepted.

A future `accepted` rule should require, at minimum:

- durable succeeded backtest evidence
- positive net edge versus baseline
- passing label scramble
- stable walk-forward evidence
- reasonable cost sensitivity
- no obvious bucket concentration problem
- explicit human or agent adversarial review notes
- replayable evidence from linked `backtest_run_id`

Until those rules exist in code and docs, no promising cell should be promoted
to `accepted`.

## What To Delete Or Defer

Defer these until the promising review exists:

- AI-generated hypothesis proposals
- larger candidate packs
- new horizons outside the canonical set
- dashboard UI work
- automatic promotion to accepted

These may become useful later, but they add search-space size before the
current evidence has been understood.

## Validation

For documentation-only changes:

```bash
git diff --check
```

For implementation that changes the research cockpit:

```bash
python scripts/research_results_report.py --check
python scripts/research_results_report.py
python -m pytest tests/test_research_results_report.py
git diff --check
```
