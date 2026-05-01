# Silver Operator Definitions

This file defines the main research-system terms in practical operator language.
Use it as the notebook layer between the database schema and the investment
workflow.

## Core Mental Model

Silver is currently a point-in-time research environment.

The basic loop is:

```text
prices -> features -> labels -> falsifier -> backtest -> verdict
```

In plain English:

1. Store historical market data.
2. Compute values that would have been known at the time.
3. Compute future outcomes so we can score ideas.
4. Test whether a feature helps predict those outcomes.
5. Compare the result to a baseline and robustness checks.
6. Record whether the hypothesis survives.

## Price Data

Price data is the raw market history stored per company per trading day.

Table:

```text
silver.prices_daily
```

Current contents:

```text
45 US equities
daily rows from 2014-04-03 through 2026-04-30
source: FMP
```

Each row stores:

```text
security_id
date
open
high
low
close
adj_close
volume
currency
available_at
```

The most important fields for the current research loop are:

```text
adj_close
volume
available_at
```

`available_at` matters because the system must know when a data value was
allowed to be used. This prevents lookahead bias.

## Labels

Labels are the future answer key.

Table:

```text
silver.forward_return_labels
```

A label says:

```text
For this company on this date, what was the future return over this horizon?
```

Current label horizons:

```text
5 trading days
21 trading days
63 trading days
126 trading days
252 trading days
```

Formula:

```text
future_return = adj_close(t + horizon) / adj_close(t) - 1
```

Example:

```text
AAPL on 2020-01-02, horizon 63
= AAPL adjusted-close return from 2020-01-02 to 63 trading days later
```

Important rule:

```text
one experiment = one horizon
```

A 63-day falsifier run only uses 63-day labels. It does not mix 5-day, 21-day,
126-day, and 252-day outcomes.

## Features

Features are known-at-the-time inputs.

Table:

```text
silver.feature_values
```

A feature says:

```text
For this company on this date, what value could we have known then?
```

Current feature definitions:

```text
momentum_12_1
momentum_6_1
return_63_0
return_21_0
avg_dollar_volume_63
realized_volatility_63
```

Features look backward or at current available data. Labels look forward.

The core test is:

```text
past/current feature -> future label
```

## momentum_12_1

`momentum_12_1` means:

```text
12 months of price momentum, skipping the most recent 1 month
```

Trading-day implementation:

```text
adj_close(t - 21) / adj_close(t - 252) - 1
```

It asks:

```text
How much did the stock move from 252 trading days ago to 21 trading days ago?
```

It skips the latest 21 trading days to avoid short-term reversal noise.

## momentum_6_1

`momentum_6_1` means:

```text
6 months of price momentum, skipping the most recent 1 month
```

Trading-day implementation:

```text
adj_close(t - 21) / adj_close(t - 126) - 1
```

It asks:

```text
How much did the stock move from 126 trading days ago to 21 trading days ago?
```

## return_63_0

`return_63_0` means:

```text
trailing 63-trading-day return with no skip
```

Formula:

```text
adj_close(t) / adj_close(t - 63) - 1
```

It asks:

```text
How much did the stock move over roughly the last quarter?
```

## return_21_0

`return_21_0` means:

```text
trailing 21-trading-day return with no skip
```

Formula:

```text
adj_close(t) / adj_close(t - 21) - 1
```

It asks:

```text
How much did the stock move over roughly the last month?
```

## avg_dollar_volume_63

`avg_dollar_volume_63` means:

```text
average daily dollar volume over the last 63 trading days
```

Formula:

```text
average(adj_close * volume)
```

It asks:

```text
How liquid or heavily traded has this stock been recently?
```

## realized_volatility_63

`realized_volatility_63` means:

```text
annualized realized volatility over the last 63 trading days
```

It uses recent daily returns, calculates their standard deviation, and
annualizes the result.

It asks:

```text
How noisy or volatile has this stock been recently?
```

## Hypotheses

Hypotheses are the investment claims being tracked.

Table:

```text
silver.hypotheses
```

In the current build, the six hypotheses map one-to-one to the six feature
ideas.

Example:

```text
feature: momentum_12_1
hypothesis: stocks with high momentum_12_1 should outperform over the chosen horizon
```

A feature is a number.

A hypothesis is a claim about what that number should predict.

## Falsifier

The falsifier is the test harness that tries to disprove a hypothesis.

For a command like:

```text
run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed
```

the falsifier asks:

```text
If we ranked companies by momentum_12_1, did the selected companies beat the
equal-weight universe over the next 63 trading days?
```

The practical steps are:

1. Load feature values for the chosen strategy.
2. Load forward-return labels for the chosen horizon.
3. Join feature rows to label rows by company and date.
4. Rank companies by feature value on each test date.
5. Select the high or low half of the universe.
6. Compare selected-company future returns against equal-weight baseline.
7. Subtract trading-cost assumptions.
8. Repeat through walk-forward time windows.
9. Run label-scramble robustness checks.
10. Store the result and verdict.

## Equal-Weight Baseline

The equal-weight baseline is the average return of all eligible companies in
the universe on the same date.

If the selected strategy rises 4 percent but the full universe rises 5 percent,
the strategy did not add value.

The falsifier compares:

```text
strategy basket return
vs.
equal-weight universe return
```

## Walk-Forward Test

Walk-forward testing checks whether a signal works consistently over time.

Instead of testing the whole history as one blob, the system breaks history
into repeated time windows.

Current default structure:

```text
minimum train window: 252 trading sessions
test window: 63 trading sessions
step size: 63 trading sessions
label gap: same as selected horizon
```

The label gap prevents the system from training on data whose future outcome
would not have been known yet.

## Label Scramble

Label scramble is a luck check.

It keeps the strategy selection pattern, but shuffles the future returns within
each test date.

It asks:

```text
Could random future returns have produced a result this good?
```

If scrambled labels often perform as well as the real labels, the signal is not
trusted.

## Analytics Runs

Analytics runs are operational job receipts.

Table:

```text
silver.analytics_runs
```

They answer:

```text
What job did the system run, with what parameters, when, and did it succeed?
```

Examples:

```text
price_normalization
label_generation
feature_generation
falsifier_report_invocation
```

### price_normalization

`price_normalization` is the job that turns vendor price data into Silver's
standard daily price table.

It reads:

```text
raw vendor price payloads
```

and writes:

```text
silver.prices_daily
```

In the current build, this means FMP daily price history is converted into one
standard row per company per trading day:

```text
open
high
low
close
adj_close
volume
currency
available_at
```

Plain English:

```text
Take the outside vendor's price response and store it in our clean internal
price format.
```

### label_generation

`label_generation` is the job that creates future-return answer keys.

It reads:

```text
silver.prices_daily
```

and writes:

```text
silver.forward_return_labels
```

For each company/date/horizon, it calculates:

```text
adj_close(t + horizon) / adj_close(t) - 1
```

Current horizons:

```text
5
21
63
126
252
```

Plain English:

```text
For each company on each date, calculate what the future return actually was.
```

These labels are not available for recent dates until enough future trading
days have passed.

### feature_generation

`feature_generation` is the job that creates known-at-the-time inputs.

It usually reads:

```text
silver.prices_daily
```

and writes:

```text
silver.feature_values
```

Examples:

```text
momentum_12_1
momentum_6_1
return_63_0
return_21_0
avg_dollar_volume_63
realized_volatility_63
```

Plain English:

```text
For each company on each date, calculate the signal value that would have been
known at that point in time.
```

Features look backward or use current available data. They do not use the
future labels.

### falsifier_report_invocation

`falsifier_report_invocation` is the job that launches a falsifier test and
records the invocation.

It reads:

```text
silver.feature_values
silver.forward_return_labels
silver.universe_membership
```

and writes or links to:

```text
silver.model_runs
silver.backtest_runs
silver.hypothesis_evaluations
markdown report files
```

Plain English:

```text
Run a historical test for one strategy, one universe, and one horizon, then
record the evidence and verdict.
```

Example:

```text
strategy: momentum_12_1
universe: falsifier_seed
horizon: 63
```

This asks:

```text
Did ranking companies by momentum_12_1 help predict 63-trading-day future
returns better than the equal-weight universe baseline?
```

Analytics runs do not store the research result itself. They store the audit
trail of system jobs.

## Model Runs

Model runs are frozen experiment setups.

Table:

```text
silver.model_runs
```

A model run records:

```text
strategy
universe
horizon
target kind
code git sha
feature set hash
input fingerprints
available_at policy versions
random seed
```

### strategy

`strategy` is the feature or rule being tested.

Current examples:

```text
momentum_12_1
avg_dollar_volume_63
realized_volatility_63
```

Plain English:

```text
What signal are we using to rank or select companies?
```

### universe

`universe` is the set of companies eligible for the test.

Current example:

```text
falsifier_seed
```

Plain English:

```text
Which companies are allowed to be included?
```

The universe matters because results are only meaningful relative to the
eligible set.

### horizon

`horizon` is the future-return period being predicted.

Current allowed horizons:

```text
5
21
63
126
252
```

Plain English:

```text
How far into the future are we measuring the outcome?
```

Example:

```text
horizon = 63
```

means:

```text
Use 63-trading-day forward returns as the answer key.
```

### target kind

`target kind` describes which type of label the model is trying to predict.

Current example:

```text
raw_return
```

Plain English:

```text
Are we predicting raw future stock return, excess return versus a benchmark, or
some other target?
```

Right now the current falsifier work is using raw forward returns.

### code git sha

`code git sha` records the exact Git commit used for the run.

Plain English:

```text
What version of the code produced this result?
```

This matters because if code changes, the same command may not mean the same
thing later.

### feature set hash

`feature set hash` is a stable fingerprint of the feature definition used in
the run.

Plain English:

```text
What exact feature definition did we test?
```

If `momentum_12_1` changes from one formula to another, the feature set hash
should change. That prevents two different calculations from being treated as
the same experiment.

### input fingerprints

`input fingerprints` are hashes of the actual joined data used by the run.

For the falsifier, the important fingerprint is currently:

```text
joined_feature_label_rows_sha256
```

Plain English:

```text
What exact rows of feature values and labels were tested?
```

This lets the system detect whether a replay is using the same data as the
original run.

### available_at policy versions

`available_at policy versions` record the timing rules used for the run.

Example:

```text
daily_price: 1
```

Plain English:

```text
Which point-in-time availability rules governed the data?
```

This matters because a feature value is only valid if the data would have been
available at that time. If the timing rule changes, the experiment identity
changes.

### random seed

`random seed` records the seed used for deterministic randomized procedures.

Current examples include label-scramble tests.

Plain English:

```text
If the run uses randomness, what seed makes it reproducible?
```

The seed lets a replay produce the same randomized checks instead of a new
random outcome.

Plain English:

```text
What exact experiment configuration did we test?
```

Even though the current strategy is simple ranking, the setup is still recorded
as a model run so it can be replayed and audited.

## Backtest Runs

Backtest runs are the measured historical results.

Table:

```text
silver.backtest_runs
```

A backtest run records:

```text
strategy return
baseline return
net difference vs baseline
walk-forward windows
label-scramble metrics
cost assumptions
failure modes
status
```

Plain English:

```text
Given the experiment setup, what happened historically?
```

Short version:

```text
model_run = setup
backtest_run = result
```

## Hypothesis Evaluations

Hypothesis evaluations are the verdict records.

Table:

```text
silver.hypothesis_evaluations
```

They connect:

```text
hypothesis
model_run
backtest_run
```

and record:

```text
accepted or rejected
failure reason
summary metrics
notes
```

Example:

```text
hypothesis: momentum_12_1
model_run_id: 21
backtest_run_id: 21
evaluation_status: rejected
failure_reason: walk_forward_unstable
```

Plain English:

```text
We tested this idea using this setup and this result, and this was the verdict.
```

## Current Database Counts

At the time this note was created, the research audit tables contained:

```text
analytics_runs: 44
model_runs: 17
backtest_runs: 17
hypotheses: 6
hypothesis_evaluations: 16
```

The count mismatch exists because the hypothesis registry was added after some
earlier backtests. So not every old backtest has a hypothesis evaluation row.

## End-To-End Example

Example question:

```text
Does high momentum_12_1 predict stronger 63-trading-day future returns?
```

The system does:

```text
1. Read historical adjusted prices.
2. Compute momentum_12_1 for each company/date.
3. Read 63-day forward-return labels for each company/date.
4. On each test date, rank companies by momentum_12_1.
5. Select the high-momentum half.
6. Measure their future 63-day return.
7. Compare against owning all 45 companies equally.
8. Repeat across walk-forward windows.
9. Scramble labels to check whether luck could explain the result.
10. Store setup, result, and verdict.
```

Core idea:

```text
known-at-the-time feature -> future return label -> tested against baseline
```
