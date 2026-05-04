# Promising Deep Dive v0

This guide defines how to inspect a promising research cell after the cockpit
labels it `deep_dive`.

The first deep dive is:

```text
avg_dollar_volume_63__h252
```

The purpose is not to prove alpha. The purpose is to understand why the cell
passed the first gates and decide whether it should continue, be watched, or be
demoted.

## Decision Anchor

Goal: explain the strongest promising cell well enough to decide the next
research action.

User value: Michael can see whether the apparent edge is a real research path,
a baseline/control exposure, or a fragile artifact.

Constraints:

- Do not promote any cell to `accepted`.
- Do not mutate hypothesis status.
- Do not add new hypotheses during the deep dive.
- Do not treat generated markdown as durable evidence.
- Keep `backtest_runs`, `model_runs`, and linked hypothesis evaluations as the
  source of truth.

Falsifier: if the result is driven by one period, one small group of tickers,
mega-cap/liquidity exposure, or fragile cost assumptions, the cell should not
advance as a research signal.

## What The Cell Means

```text
avg_dollar_volume_63__h252
```

Breakdown:

| Part | Meaning |
| --- | --- |
| `avg_dollar_volume_63` | Feature: trailing 63-session average of adjusted close times volume. |
| `high` | Selection direction: buy/select the high side of the feature. |
| `h252` | Horizon: measure forward return 252 trading sessions after the as-of date. |

Plain English:

```text
Among the eligible universe, select the stocks with the highest recent dollar
trading volume, then measure performance over the next 252 trading sessions.
```

High dollar volume often overlaps with:

- large market capitalization
- high liquidity
- high institutional ownership
- popular mega-cap stocks
- lower trading friction
- market-regime leadership

The deep dive must determine whether this is a distinct useful signal or only a
proxy for a baseline exposure.

## Current Cockpit Evidence

The current generated cockpit summarizes the cell as:

| Field | Current value |
| --- | --- |
| Recommendation | `deep_dive` |
| Horizon | 252 trading sessions |
| Edge | +1.6025% versus equal-weight baseline |
| Buckets | 21/35 positive |
| Label scramble | passed |
| Cost sensitivity | low, about 8.0x current round-trip cost |
| Adjacent horizon | `h126` rejected for `walk_forward_unstable` |

This evidence is enough to inspect. It is not enough to accept.

## Deep-Dive Questions

The deep dive should answer these questions in order.

| Question | What To Look For | Bad Sign |
| --- | --- | --- |
| Which years drove the edge? | Edge by calendar year or bucket year. | Most edge comes from one year or one regime. |
| Which buckets failed? | Negative walk-forward windows and their dates. | Failures cluster in recent data or normal regimes. |
| Which tickers were selected most often? | Selection frequency by ticker. | One or two tickers dominate the result. |
| Is this just mega-cap/liquidity exposure? | Selected tickers are mostly the largest or most liquid names. | The signal is a disguised size/liquidity baseline. |
| Why did 252 pass while 126 failed? | Compare h126 and h252 bucket patterns and mean edge. | h252 works only because of delayed, concentrated wins. |
| Would higher costs erase it? | Recompute or approximate edge under larger cost assumptions. | Edge disappears under modest cost increases. |
| Does it overlap with momentum? | Compare selected names and positive buckets with momentum cells. | The cell is just a momentum proxy. |

## Generated Output

The research cockpit now generates a read-only `Promising Deep Dive v0` section
inside `reports/research/results_v0.md` when this stored cell exists:

```text
Promising Deep Dive v0

Cell: avg_dollar_volume_63__h252
Recommendation: continue | watch | demote
Reason: one-sentence operator reason

Summary:
- edge:
- bucket breadth:
- concentration:
- adjacent horizon read:
- cost sensitivity:
- overlap risk:

Year/Bucket Drivers:
...

Selected Tickers:
...

Exposure Notes:
...

Decision:
...
```

The recommendation is a deep-dive recommendation, not a registry status.

The v0 implementation uses stored report evidence only:

- bucket/year drivers from `backtest_runs.metrics.walk_forward_windows`
- current edge versus the equal-weight baseline
- current cost sensitivity
- adjacent horizon rows for the same base hypothesis
- a pattern-only momentum proxy using stored momentum horizon rows
- selected-ticker attribution reconstructed from persisted feature values,
  forward-return labels, and the stored walk-forward windows
- same-horizon selected-ticker overlap versus momentum cells

The selected-ticker attribution is read-only reconstruction. It does not create
predictions, portfolio rows, or new evidence tables. It answers:

- which tickers were selected most often
- how concentrated selection was
- whether the same selected tickers also appear in same-horizon momentum cells

If attribution cannot be reconstructed, the generated section must say:

```text
Selected Tickers:
- not_available: stored selection attribution is not available in current report rows.
```

## Recommendation Labels

| Label | Meaning |
| --- | --- |
| `continue` | The cell deserves more research work, such as a broader universe, richer controls, or a formal baseline comparison. |
| `watch` | The cell is interesting but has enough fragility that it should remain visible without driving the next build. |
| `demote` | The deep dive found the promising result too fragile or too redundant to guide new work. |

Default to `watch` when evidence is mixed.

## Initial Recommendation Rules

These are operator rules for the deep dive. They should be documented before
being automated.

`continue` if:

- edge is spread across multiple years
- positive buckets are not concentrated in one period
- selected tickers are not overly concentrated
- the result is not only mega-cap exposure
- h126 weakness is explainable rather than contradictory
- edge survives reasonable cost stress
- the behavior is meaningfully different from momentum

`watch` if:

- edge is positive but concentrated
- selected tickers are dominated by a few names
- the result looks like a useful baseline/control but not a research signal
- cost stress is unclear
- overlap with momentum is unclear

`demote` if:

- one year, one regime, or one ticker explains most of the edge
- higher costs erase the edge
- the result is essentially a size/liquidity baseline
- h252 success contradicts adjacent horizons with no explanation
- the same names and buckets are already captured by momentum

## Data Needed

The deep dive should read existing stored evidence first.

| Data | Source |
| --- | --- |
| Bucket dates and returns | `backtest_runs.metrics.walk_forward_windows` |
| Strategy and baseline returns | `backtest_runs.metrics` and `backtest_runs.baseline_metrics` |
| Label scramble | `backtest_runs.label_scramble_metrics` |
| Costs | `backtest_runs.cost_assumptions` |
| Selected securities by as-of date | Reconstructed from `feature_values`, `forward_return_labels`, `universe_membership`, and `backtest_runs.metrics.walk_forward_windows`. |
| Adjacent horizon rows | `avg_dollar_volume_63__h126` and existing 63-day row |
| Momentum comparison | `momentum_12_1`, `momentum_6_1`, and `momentum_3_0` rows at nearby horizons |

The attribution is not causal decomposition. It explains repeated selected
exposure and overlap. It does not prove that a ticker caused the edge.

## What Not To Do

Do not:

- promote `avg_dollar_volume_63__h252` to `accepted`
- add new candidate features
- add non-canonical horizons
- tune thresholds to make the cell look better
- call the result alpha
- claim ticker concentration without stored selection evidence
- replace the equal-weight baseline before explaining the current result

## Build Order

Use the smallest useful build:

1. [x] Add a readable generated `Promising Deep Dive` section for
   `avg_dollar_volume_63__h252`.
2. [x] Include bucket/year contribution and adjacent-horizon comparison.
3. [x] Show current cost sensitivity from stored cost assumptions.
4. [x] State plainly when ticker-selection attribution is unavailable.
5. [x] Add ticker-selection attribution only if stored evidence supports it.
6. [x] Add same-horizon momentum selected-ticker overlap.
7. [ ] Add richer cost stress only after the current cost assumptions are shown.
8. [ ] Only then consider generalizing the deep-dive code to other cells.

Do not start with a generalized dashboard. One inspected survivor is enough for
v0.

## Validation

For documentation-only changes:

```bash
git diff --check
```

For implementation:

```bash
python scripts/research_results_report.py --check
python scripts/research_results_report.py
python -m pytest tests/test_research_results_report.py
git diff --check
```
