# Silver — Build Specification v1.0

> *"For each security, on each as-of date: what was knowable, what features existed, what did Silver predict, what portfolio would it hold, and what happened?"*

---

## 1. Mission

Silver is a point-in-time prediction and backtesting system for US equity forward returns. Its purpose is to determine, with rigor and reproducibility, whether modern AI-extracted signals can predict stock returns net of trading costs better than well-known numeric baselines.

Silver is not a research dashboard, an analyst chatbot, a valuation engine, or a portfolio reporting tool. It is a falsification machine for one investment thesis.

**The thesis being tested:**

> Modern LLMs can extract structured signals from transcripts, filings, and news at a scale humans cannot, and at least some of those signals may improve forward-return prediction when tested rigorously against future prices, after costs, across regimes.

**The success criterion:**

> AI-derived text features improve out-of-sample, net-of-cost prediction over numeric-only baselines, and survive label-scramble tests, multiple-comparisons correction, and adversarial review.

If the thesis fails, Silver should fail it cleanly and report the failure. The system is engineered to admit "no edge" as a valid output.

---

## 2. Scope and Boundaries

**In scope:**
- US-listed common equities and ADRs trading on NYSE / Nasdaq / NYSE-Arca / NYSE-American
- Daily-frequency observations
- Forward-return horizons of 5, 21, 63, 126, and 252 trading days
- Initial universe of ~40–50 tickers for falsifier; expand to ~500 if falsifier survives
- Numeric features (deterministic from prices + fundamentals)
- Text features (LLM-extracted from transcripts, filings, news)
- Walk-forward backtests with realistic transaction costs
- Paper trading simulation
- Hypothesis generation, validation, and lifecycle

**Out of scope (v1):**
- Live capital deployment
- Options, futures, fixed income, crypto, foreign equities
- Intraday-frequency signals
- Real-time market microstructure
- Reinforcement learning
- Manual analyst override workflows
- Web UI (CLI scripts only until paper trading runtime)
- Multi-user collaboration
- News from non-public social channels
- Insider transaction analytics (deferred phase 2+)
- Macro regime models beyond manual era splits

**Allowed external dependencies:**
- FMP (Financial Modeling Prep) for fundamentals, transcripts, prices, corporate actions
- SEC EDGAR for raw filings and XBRL companyfacts
- Optional: Arrow's local raw data caches under `~/Arrow/data/raw/` as a vendor-byte mirror to skip rate-limited re-fetching
- Optional: Norgate Premium Data for delisting history when scaling beyond falsifier

**Disallowed dependencies:**
- Arrow's analyst-facing views (`v_company_period_wide`, `v_metrics_*`, etc.)
- Arrow's normalization opinions (`extraction_version` preference order, supersession chains)
- Arrow's Python code or schema imports
- Any consumer-facing third-party dashboard

---

## 3. Three Laws (Day-One Invariants)

These are non-negotiable and enforced in code:

1. **No feature without `available_at`.** Every feature value carries a timestamp earlier than which the value is invisible to any backtest.
2. **No prediction without a frozen feature version.** Predictions reference exact `feature_definition_id` + `model_version` + `prompt_version` tuples. Re-running a backtest yields identical predictions.
3. **No backtest result without costs, baseline comparison, and reproducibility metadata.** Every reported metric is net of transaction costs, accompanied by at least one baseline (numeric-only ensemble), and traceable to a specific `model_run_id` with `code_git_sha`, feature set hash, training window, random seed, and execution-assumption set.

Violation of any law invalidates downstream claims. The system refuses to write results that violate the laws.

---

## 4. Native Objects

The world Silver models:

| Object | Definition |
|---|---|
| **Security** | A tradable equity instrument (ticker + identifier history) |
| **Universe** | A point-in-time set of securities Silver considers eligible |
| **Trading calendar** | Days the US equity market is open, including early closes |
| **Event** | Something newly observable about a security (earnings call, 8-K, news item) |
| **Artifact** | Source material attached to an event (transcript text, 10-K HTML, news article body) |
| **Raw object** | Verbatim vendor response with cryptographic hash, request fingerprint, fetched_at |
| **Fundamental fact** | Numeric financial datum (revenue, COGS, etc.) tagged with PIT timestamps |
| **Price** | Daily OHLCV + adjusted-close, with corporate-action history |
| **Feature** | Numeric signal (deterministic or AI-extracted) computable as of any asof_date |
| **Label** | Realized forward return at a horizon, computed after the horizon elapses |
| **Hypothesis** | A testable rule mapping (feature combinations) → predicted excess return |
| **Prediction** | A frozen model output for (security, asof_date, horizon), made before the label was known |
| **Backtest run** | A reproducible execution of a hypothesis against a date range with full metadata |
| **Model run** | A specific (code_sha, feature_set, training_window, seed) tuple producing predictions |
| **Portfolio** | A weighted basket of positions (simulated or paper) on a given date |
| **Outcome** | Scored result of a prediction or portfolio against realized returns |
| **Risk event** | An automatic control trigger (drawdown halt, capacity breach, data quality block) |

Anything not in this list is a feature of one of these objects, not a primitive.

---

## 5. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      EXTERNAL SOURCES                           │
│  FMP  •  SEC EDGAR  •  News vendor (deferred)                   │
│                                                                  │
│  (Optional bootstrap: Arrow's ~/Arrow/data/raw/ as local mirror)│
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    INGEST LAYER (clients + raw vault)           │
│  • FMP client  • SEC client  • Polite HTTP w/ rate limit       │
│  • RawVault    →  silver.raw_objects (immutable, hashed)       │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│              POINT-IN-TIME NORMALIZATION LAYER                   │
│  Source adapters convert raw objects to Silver-native rows.      │
│  available_at policy applied per source.                         │
│                                                                  │
│  →  events  →  artifacts  →  fundamental_facts                  │
│  →  prices_daily  →  corporate_actions                          │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                          FEATURE STORE                           │
│                                                                  │
│  ┌────────────────────────┐    ┌────────────────────────┐       │
│  │  NUMERIC FEATURES      │    │  TEXT FEATURES         │       │
│  │  Deterministic from    │    │  AI-extracted from     │       │
│  │  facts + prices        │    │  artifacts via LLM     │       │
│  │                        │    │                        │       │
│  │  Computed on demand;   │    │  Computed once at      │       │
│  │  immutable per         │    │  artifact ingest;      │       │
│  │  feature_definition    │    │  immutable per         │       │
│  │  version               │    │  (model_version,       │       │
│  │                        │    │   prompt_version)      │       │
│  └────────────────────────┘    └────────────────────────┘       │
│                          ↓                                       │
│            silver.feature_values (versioned, PIT)               │
│            silver.feature_snapshots (frozen for replay)         │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                          LABELS                                  │
│  Forward returns at 5/21/63/126/252 trading days; raw/excess   │
│  Computed deterministically from prices + corporate_actions     │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BACKTEST ENGINE                             │
│  Walk-forward CV  •  Regime slices  •  Transaction costs       │
│  Baseline comparison  •  Label-scramble tests                   │
│  Multiple-comparisons correction  •  Capacity estimates         │
│  Drawdown / factor-exposure metrics                             │
│                                                                  │
│  →  silver.backtest_runs  →  silver.model_runs                  │
│  →  silver.predictions   →  silver.prediction_outcomes          │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                  HYPOTHESIS MACHINE                              │
│                                                                  │
│  LLM proposes  →  Adversarial critic  →  Backtest validates    │
│  Lifecycle: candidate → validated → live → retired              │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                  PORTFOLIO LAYER                                 │
│  Sim portfolio (historical backtest)                            │
│  Paper portfolio (real-time forward test)                       │
│  Risk controls: position / sector / turnover / drawdown halt    │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│              RUNTIME JOBS  (daily, weekly, monthly)              │
└─────────────────────────────────────────────────────────────────┘
```

**Cross-cutting concerns:**
- **LLM model router** — three-tier escalation (local 8B → cloud 70B → frontier) with cost/latency/failure tracking
- **Reproducibility contract** — every result row carries (code_sha, feature_set_hash, model_run_id)
- **Data quality findings** — inserted at any layer; can gate predictions

---

## 6. Time Discipline

### 6.1 Five canonical timestamps

| Timestamp | Definition |
|---|---|
| `event_at` | When the underlying event happened in the world (e.g., the moment NVDA's CEO answered a question on the call) |
| `published_at` | When the source claims the information became public (e.g., the SEC filing's accepted_at, the press release's release time) |
| `available_at` | The earliest time Silver is permitted to use this information in a historical simulation. This is the load-bearing field. |
| `ingested_at` | When Silver fetched it (now-ish for fresh data, archived for backfilled data) |
| `asof_date` | The prediction date — the simulated "today" of a backtest |

A backtest at `asof_date = D` may use any datum where `available_at ≤ D`. That is the only rule. Lookahead bias is the violation of this rule, and it is the single most common cause of backtest fraud (intentional or not).

### 6.2 `available_at` policy table

This is encoded as a database table, not as code or folklore. Policy versions are auditable.

| Source | available_at = | Rationale |
|---|---|---|
| Daily price | `date + 18:00 ET` | Post-close + buffer for adjustments |
| 10-K filing | `accepted_at + 1 trading day at 09:30 ET` | Filings drop after market close; earliest action is next open |
| 10-Q filing | Same as 10-K | Same logic |
| 8-K filing (material) | `accepted_at + 30 minutes` | Material 8-Ks move prices intraday |
| Earnings call transcript | `call_end_time + 2 hours` (or `fetched_at` if no call_end_time) | Vendor delay typical |
| Press release (timestamped) | `release_time + 5 minutes` | Wire delivery latency |
| Press release (date only) | Next trading day at 09:30 ET | Conservative when time unknown |
| Fundamental fact derived from filing | Inherits filing's `available_at` | Cannot be more recent than its source |
| FMP profile / static data | `fetched_at` (treat as available immediately) | Reference data, not predictive |
| Corporate action | `ex_date + 09:30 ET` | Take effect at next open |
| News (timestamped) | `published_at + 5 minutes` | Aggregator latency |
| XBRL companyfacts | `filing.accepted_at + 1 trading day` | Same as the underlying filing |

**Policy versioning rule:** Changing any rule above creates a new `available_at_policy_id`. Every fact, feature, and label records the policy version under which its `available_at` was computed. Backtests reference the policy version active at the time of computation.

### 6.3 Replay invariant

```
For any (asof_date D, code_git_sha S, available_at_policy_version V):
  re-running the system produces byte-identical predictions to the
  predictions originally made at D under S and V.
```

This invariant is the operational definition of reproducibility. Violation is a P0 bug.

---

## 7. Database Schema

All tables in `silver` schema. Single Postgres instance; database name `silver`. DDL applied via numbered migrations under `db/migrations/`.

### 7.1 Securities and identifiers

```sql
CREATE TABLE silver.securities (
    id                  bigserial PRIMARY KEY,
    ticker              text NOT NULL UNIQUE,
    name                text NOT NULL,
    cik                 text,
    exchange            text,
    asset_class         text NOT NULL DEFAULT 'equity',
    country             text NOT NULL DEFAULT 'US',
    currency            text NOT NULL DEFAULT 'USD',
    fiscal_year_end_md  text,                    -- 'MM-DD'
    listed_at           date,                    -- IPO / first available
    delisted_at         date,                    -- NULL = active
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE silver.security_identifiers (
    security_id     bigint NOT NULL REFERENCES silver.securities(id),
    identifier_type text NOT NULL,           -- ticker, cik, isin, cusip, fmp_symbol
    identifier      text NOT NULL,
    valid_from      date NOT NULL,
    valid_to        date,                    -- NULL = current
    PRIMARY KEY (security_id, identifier_type, valid_from)
);
```

### 7.2 Trading calendar and universe

```sql
CREATE TABLE silver.trading_calendar (
    date            date PRIMARY KEY,
    is_session      boolean NOT NULL,
    session_close   timestamptz,
    is_early_close  boolean NOT NULL DEFAULT false
);

CREATE TABLE silver.universe_membership (
    security_id    bigint NOT NULL REFERENCES silver.securities(id),
    universe_name  text NOT NULL,           -- 'falsifier', 'sp500', etc.
    valid_from     date NOT NULL,
    valid_to       date,
    reason         text,
    PRIMARY KEY (security_id, universe_name, valid_from)
);
```

### 7.3 Raw vault

```sql
CREATE TABLE silver.raw_objects (
    id            bigserial PRIMARY KEY,
    vendor        text NOT NULL,           -- fmp, sec, fred, news_*, etc.
    endpoint      text NOT NULL,
    params_hash   text NOT NULL,           -- sha256 of canonical params
    params        jsonb NOT NULL,
    request_url   text NOT NULL,           -- secrets stripped
    http_status   integer NOT NULL,
    content_type  text,
    body_jsonb    jsonb,
    body_raw      bytea,
    raw_hash      text NOT NULL,           -- sha256 of body
    metadata      jsonb NOT NULL DEFAULT '{}'::jsonb,
    fetched_at    timestamptz NOT NULL DEFAULT now()
);
```

### 7.4 Time policy

```sql
CREATE TABLE silver.available_at_policies (
    id          bigserial PRIMARY KEY,
    name        text NOT NULL,             -- e.g., '10K_filing'
    version     integer NOT NULL,
    rule        jsonb NOT NULL,            -- structured rule definition
    valid_from  timestamptz NOT NULL DEFAULT now(),
    valid_to    timestamptz,               -- NULL = current
    notes       text,
    UNIQUE (name, version)
);
```

### 7.5 Events and artifacts

```sql
CREATE TABLE silver.events (
    id              bigserial PRIMARY KEY,
    security_id     bigint REFERENCES silver.securities(id),
    event_type      text NOT NULL,         -- earnings_call, 10k, 10q, 8k, news, etc.
    event_at        timestamptz NOT NULL,
    published_at    timestamptz NOT NULL,
    available_at    timestamptz NOT NULL,
    available_at_policy_id bigint REFERENCES silver.available_at_policies(id),
    raw_object_id   bigint REFERENCES silver.raw_objects(id),
    summary         text,
    evidence        jsonb
);

CREATE TABLE silver.artifacts (
    id              bigserial PRIMARY KEY,
    security_id     bigint NOT NULL REFERENCES silver.securities(id),
    artifact_type   text NOT NULL,         -- transcript, 10k_filing, 10q_filing, news, etc.
    source          text NOT NULL,
    period_end      date,
    published_at    timestamptz NOT NULL,
    available_at    timestamptz NOT NULL,
    available_at_policy_id bigint REFERENCES silver.available_at_policies(id),
    raw_object_id   bigint REFERENCES silver.raw_objects(id),
    content_text    text,
    raw_hash        text NOT NULL,
    metadata        jsonb
);

-- chunked text for FTS / feature extraction
CREATE TABLE silver.artifact_chunks (
    id              bigserial PRIMARY KEY,
    artifact_id     bigint NOT NULL REFERENCES silver.artifacts(id),
    chunk_ordinal   integer NOT NULL,
    chunk_kind      text NOT NULL,         -- speaker_turn, mda_section, etc.
    text            text NOT NULL,
    metadata        jsonb,
    UNIQUE (artifact_id, chunk_ordinal)
);
```

### 7.6 Prices and corporate actions

```sql
CREATE TABLE silver.prices_daily (
    security_id     bigint NOT NULL REFERENCES silver.securities(id),
    date            date NOT NULL,
    open            numeric(18,6),
    high            numeric(18,6),
    low             numeric(18,6),
    close           numeric(18,6),
    adj_close       numeric(18,6) NOT NULL,
    volume          bigint,
    available_at    timestamptz NOT NULL,
    raw_object_id   bigint REFERENCES silver.raw_objects(id),
    PRIMARY KEY (security_id, date)
);

CREATE TABLE silver.corporate_actions (
    id              bigserial PRIMARY KEY,
    security_id     bigint NOT NULL REFERENCES silver.securities(id),
    action_type     text NOT NULL,         -- split, dividend, spin, merger
    ex_date         date NOT NULL,
    value           numeric(18,6),
    raw_object_id   bigint REFERENCES silver.raw_objects(id),
    UNIQUE (security_id, action_type, ex_date)
);
```

### 7.7 Fundamental facts (Silver-native)

```sql
CREATE TABLE silver.fundamental_facts (
    id                  bigserial PRIMARY KEY,
    security_id         bigint NOT NULL REFERENCES silver.securities(id),
    concept             text NOT NULL,
    value               numeric NOT NULL,
    unit                text NOT NULL DEFAULT 'USD',
    fiscal_year         integer NOT NULL,
    fiscal_quarter      integer,           -- NULL for annual
    period_end          date NOT NULL,
    period_type         text NOT NULL,     -- annual, quarter
    calendar_year       integer NOT NULL,
    calendar_quarter    integer,
    published_at        timestamptz NOT NULL,
    available_at        timestamptz NOT NULL,
    available_at_policy_id bigint REFERENCES silver.available_at_policies(id),
    raw_object_id       bigint REFERENCES silver.raw_objects(id),
    supersedes_id       bigint REFERENCES silver.fundamental_facts(id),
    superseded_at       timestamptz,
    source_system       text NOT NULL,     -- 'fmp', 'sec_xbrl', 'arrow_cache'
    normalization_version text NOT NULL,
    notes               text
);

-- Partial unique: at most one current row per business identity
CREATE UNIQUE INDEX fundamental_facts_one_current_idx
    ON silver.fundamental_facts (
        security_id, concept, period_end, period_type, normalization_version
    )
    WHERE superseded_at IS NULL;
```

### 7.8 Feature store

```sql
CREATE TABLE silver.feature_definitions (
    id              bigserial PRIMARY KEY,
    name            text NOT NULL,          -- e.g., 'momentum_12_1'
    version         integer NOT NULL,       -- version of this feature's logic
    kind            text NOT NULL,          -- numeric, text
    computation_spec jsonb NOT NULL,        -- structured: SQL or LLM prompt ref
    model_version   text,                   -- for text features
    prompt_version  text,                   -- for text features
    created_at      timestamptz NOT NULL DEFAULT now(),
    notes           text,
    UNIQUE (name, version)
);

CREATE TABLE silver.feature_values (
    id                  bigserial PRIMARY KEY,
    security_id         bigint NOT NULL REFERENCES silver.securities(id),
    asof_date           date NOT NULL,
    feature_definition_id bigint NOT NULL REFERENCES silver.feature_definitions(id),
    value               double precision,   -- NULL = not computable / insufficient data
    confidence          double precision,   -- for text features
    source_event_id     bigint REFERENCES silver.events(id),
    source_artifact_id  bigint REFERENCES silver.artifacts(id),
    computed_at         timestamptz NOT NULL DEFAULT now(),
    UNIQUE (security_id, asof_date, feature_definition_id)
);

-- Snapshots: frozen feature value sets for backtest replay
CREATE TABLE silver.feature_snapshots (
    id                  bigserial PRIMARY KEY,
    name                text NOT NULL,      -- 'falsifier_v1', 'numeric_only', etc.
    feature_set_hash    text NOT NULL,      -- sha256 of the feature set
    feature_definition_ids bigint[] NOT NULL,
    asof_min            date NOT NULL,
    asof_max            date NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (feature_set_hash)
);
```

### 7.9 Labels

```sql
CREATE TABLE silver.labels (
    id              bigserial PRIMARY KEY,
    security_id     bigint NOT NULL REFERENCES silver.securities(id),
    label_date      date NOT NULL,          -- the prediction-anchor date
    horizon_days    integer NOT NULL,       -- 5, 21, 63, 126, 252
    target_kind     text NOT NULL,          -- raw_return, excess_return_market, excess_return_sector
    value           double precision,       -- NULL = horizon not yet elapsed
    benchmark_ticker text,                  -- for excess returns
    computed_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (security_id, label_date, horizon_days, target_kind)
);
```

### 7.10 Model registry, backtests, predictions, outcomes

```sql
CREATE TABLE silver.model_runs (
    id                  bigserial PRIMARY KEY,
    model_run_key       text NOT NULL UNIQUE,
    name                text NOT NULL,
    code_git_sha        text NOT NULL,
    feature_set_hash    text NOT NULL,
    feature_snapshot_ref text,
    training_start_date date NOT NULL,
    training_end_date   date NOT NULL,
    test_start_date     date NOT NULL,
    test_end_date       date NOT NULL,
    horizon_days        integer NOT NULL,
    target_kind         text NOT NULL,
    random_seed         integer NOT NULL,
    cost_assumptions    jsonb NOT NULL DEFAULT '{}'::jsonb,
    parameters          jsonb NOT NULL DEFAULT '{}'::jsonb,
    metrics             jsonb NOT NULL DEFAULT '{}'::jsonb,
    available_at_policy_versions jsonb NOT NULL DEFAULT '{}'::jsonb,
    input_fingerprints  jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at          timestamptz NOT NULL DEFAULT now(),
    finished_at         timestamptz,
    status              text NOT NULL DEFAULT 'running',
    created_at          timestamptz NOT NULL DEFAULT now(),
    CHECK (cost_assumptions <> '{}'::jsonb),
    CHECK (available_at_policy_versions <> '{}'::jsonb),
    CHECK (
        feature_snapshot_ref IS NOT NULL
        OR input_fingerprints <> '{}'::jsonb
    )
);

CREATE TABLE silver.backtest_runs (
    id                  bigserial PRIMARY KEY,
    backtest_run_key    text NOT NULL UNIQUE,
    model_run_id        bigint NOT NULL REFERENCES silver.model_runs(id) ON DELETE RESTRICT,
    name                text NOT NULL,
    universe_name       text NOT NULL,
    horizon_days        integer NOT NULL,
    target_kind         text NOT NULL,
    cost_assumptions    jsonb NOT NULL DEFAULT '{}'::jsonb,
    parameters          jsonb NOT NULL DEFAULT '{}'::jsonb,
    metrics             jsonb NOT NULL DEFAULT '{}'::jsonb,
    metrics_by_regime   jsonb NOT NULL DEFAULT '{}'::jsonb,
    baseline_metrics    jsonb NOT NULL DEFAULT '{}'::jsonb,
    label_scramble_metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    label_scramble_pass boolean,
    multiple_comparisons_correction text,   -- 'bh', 'bonferroni', 'none'
    started_at          timestamptz NOT NULL DEFAULT now(),
    finished_at         timestamptz,
    status              text NOT NULL DEFAULT 'running',
    created_at          timestamptz NOT NULL DEFAULT now(),
    CHECK (cost_assumptions <> '{}'::jsonb),
    CHECK (
        status <> 'succeeded'
        OR (
            cost_assumptions <> '{}'::jsonb
            AND metrics <> '{}'::jsonb
            AND metrics_by_regime <> '{}'::jsonb
            AND baseline_metrics <> '{}'::jsonb
            AND label_scramble_metrics <> '{}'::jsonb
        )
    )
);

CREATE TABLE silver.predictions (
    id                  bigserial PRIMARY KEY,
    prediction_date     date NOT NULL,
    security_id         bigint NOT NULL REFERENCES silver.securities(id),
    horizon_days        integer NOT NULL,
    target_kind         text NOT NULL,
    predicted_value     double precision NOT NULL,
    confidence          double precision,
    model_run_id        bigint NOT NULL REFERENCES silver.model_runs(id),
    hypothesis_id       bigint REFERENCES silver.hypotheses(id),
    feature_snapshot_id bigint REFERENCES silver.feature_snapshots(id),
    rationale           jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (prediction_date, security_id, horizon_days, target_kind, model_run_id)
);

CREATE TABLE silver.prediction_outcomes (
    prediction_id       bigint PRIMARY KEY REFERENCES silver.predictions(id) ON DELETE CASCADE,
    realized_value      double precision NOT NULL,
    error               double precision NOT NULL,  -- predicted - realized
    label_id            bigint REFERENCES silver.labels(id),
    scored_at           timestamptz NOT NULL DEFAULT now()
);
```

The durable backtest metadata registry is the pair
`silver.model_runs` + `silver.backtest_runs`. A backtest claim is accepted only
when the `backtest_runs` row is terminal with `status = 'succeeded'` and the
row has non-empty headline metrics, cost assumptions, baseline metrics, regime
metrics, label-scramble evidence, and the multiple-comparisons setting used for
that claim. `failed` and `insufficient_data` rows are audit evidence, not
accepted alpha claims; `running` rows must not be reported as results.

The stable claim identity is `backtest_runs.id` plus `backtest_run_key`. It must
resolve through `backtest_runs.model_run_id` to exactly one `model_runs` row
containing the frozen code SHA, feature-set hash, feature snapshot or input
fingerprints, training/test window, horizon, target kind, random seed,
cost/execution assumptions, parameters, available-at policy versions, and final
model-run status. Reports may echo command-line metadata, but the registry rows
are the source of truth for reproducing or rejecting a reported backtest.

#### Backtest replay from run identity

Accepted-claim replay starts from a durable backtest identity:
`backtest_run_id` or `backtest_run_key`. Replay must load the matching
`backtest_runs` row and the exact joined `model_runs` row before any rerun or
dry-run comparison. A `model_run_id` alone can reconstruct model and prediction
identity, but it is not a complete accepted backtest claim because multiple
backtest rows can legitimately reference the same model run.

The deterministic replay contract is the union of:

- model identity fields: `model_run_key`, `code_git_sha`, `feature_set_hash`,
  `feature_snapshot_ref` when present, `input_fingerprints`, training/test
  dates, `horizon_days`, `target_kind`, `random_seed`, `cost_assumptions`,
  normalized model parameters, available-at policy versions, model metrics, and
  terminal model status
- backtest identity and evidence fields: `backtest_run_key`, the durable
  `model_run_id` join, `universe_name`, `horizon_days`, `target_kind`,
  `cost_assumptions`, normalized backtest parameters, headline metrics, regime
  metrics, baseline metrics, label-scramble metrics, `label_scramble_pass`,
  multiple-comparisons setting, and terminal backtest status

Replay must reject, not patch over, any missing row, broken join, missing
replay input, non-terminal accepted-claim row, non-`succeeded` accepted-claim
row, changed deterministic key, changed policy/cost/config/input field, or
changed report-critical metric. The rejection must name the mismatched field
well enough for an operator or merge steward to audit it. Replay must never
silently substitute current CLI defaults, current available-at policies,
current feature definitions, current vendor data, or fresh invocation metadata
for the values recorded on the original run.

Non-goals for replay from run identity: no live or paid vendor fetches, no
rewriting applied migrations, no feature-family expansion, no regeneration from
newer feature definitions, no portfolio or paper-trading execution, and no use
of UUIDs, process ids, host/user names, timestamps, output paths, report paths,
or database surrogate ids as deterministic run identity fields.

#### Falsifier run identity

For `scripts/run_falsifier.py`, `model_run_key` is a deterministic model
identity, not a fresh invocation identity. Re-running the same falsifier at the
same code and data state must produce the same `model_run_key`.

The `model_run_key` digest input is the canonical JSON form of these stable
fields, with object keys sorted before hashing:

- contract version for the falsifier identity payload
- `code_git_sha`
- normalized run config: `strategy`, `universe_name`, `horizon_days`, and
  `target_kind`
- `feature_set_hash` and the feature-definition identity used to derive it
- joined persisted input fingerprint, specifically
  `input_fingerprints.joined_feature_label_rows_sha256`
- model training and test windows, including the window source
- walk-forward config that changes scored predictions, including minimum train
  sessions, test sessions, and step sessions
- random seed
- model-run cost and execution assumptions recorded in `model_runs`
- available-at policy version mapping

The digest must not include a UUID, process id, wall-clock start time, duration,
host/user name, database surrogate id, output path, report path, or CLI spelling
that does not change the normalized run config. The `model_runs` create payload
must also stay byte-stable for the same deterministic identity, because the
repository treats a repeated key with different metadata as a contract
violation.

Fresh invocation metadata, if retained, is audit metadata outside deterministic
run identity. It may be stored in an append-only `silver.analytics_runs` row
with `run_kind = 'backtest'` for durable backtest execution or
`run_kind = 'falsifier_report_invocation'` for report-generation audit
metadata, including the resolved `model_run_key` / `backtest_run_key` in
`parameters`, or in a later dedicated invocation table.
It must not be written into the immutable `model_runs` or `backtest_runs`
create payload for a deterministic key. The existing registry schema is
sufficient for this contract; a schema change is required only if downstream
work needs queryable per-invocation foreign keys instead of JSON audit
metadata.

The `backtest_run_key` is deterministic from the resolved `model_run_key` plus
the normalized backtest/report config that affects accepted evidence, including
universe, horizon, strategy, label-scramble settings, multiple-comparisons
setting, cost assumptions, and contract version.

### 7.11 Hypotheses

The implemented hypothesis registry v0 stores testable candidate ideas and the
replayable backtest evidence currently attached to each idea. Lifecycle-event
automation can be added later, but the manual operator path comes first.

```sql
CREATE TABLE silver.hypotheses (
    id              bigserial PRIMARY KEY,
    hypothesis_key  text NOT NULL UNIQUE,
    name            text NOT NULL,
    thesis          text NOT NULL,
    signal_name     text NOT NULL,
    mechanism       text NOT NULL,
    universe_name   text,
    horizon_days    integer,
    target_kind     text,
    status          text NOT NULL DEFAULT 'proposed',
                    -- proposed, running, rejected, promising, accepted, retired
    metadata        jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE silver.hypothesis_evaluations (
    id              bigserial PRIMARY KEY,
    hypothesis_id   bigint NOT NULL REFERENCES silver.hypotheses(id),
    model_run_id    bigint NOT NULL REFERENCES silver.model_runs(id),
    backtest_run_id bigint NOT NULL REFERENCES silver.backtest_runs(id),
    evaluation_status text NOT NULL,
                    -- running, rejected, promising, accepted, failed
    failure_reason  text,
    notes           text,
    summary_metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (hypothesis_id, backtest_run_id)
);
```

An evaluation is not the canonical backtest result. It is an index row pointing
from a hypothesis to `backtest_runs`, whose joined `model_runs` row remains the
source of truth for costs, baselines, feature identity, policy versions, replay
inputs, and report-critical metrics.

### 7.12 Portfolios and execution

```sql
CREATE TABLE silver.execution_assumptions (
    id                  bigserial PRIMARY KEY,
    name                text NOT NULL UNIQUE,
    spread_bps          numeric NOT NULL,         -- half-spread in bps
    impact_model        jsonb NOT NULL,           -- e.g., {kind: 'sqrt', coefficient: 0.1}
    borrow_bps_annual   numeric NOT NULL DEFAULT 25,
    fill_convention     text NOT NULL,            -- 'next_open', 'eod_close', etc.
    min_dollar_volume   numeric,                  -- liquidity filter
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE silver.portfolios (
    id              bigserial PRIMARY KEY,
    name            text NOT NULL UNIQUE,
    kind            text NOT NULL,           -- 'sim', 'paper', 'live'
    started_at      timestamptz NOT NULL DEFAULT now(),
    notes           text
);

CREATE TABLE silver.portfolio_positions (
    portfolio_id    bigint NOT NULL REFERENCES silver.portfolios(id),
    asof_date       date NOT NULL,
    security_id     bigint NOT NULL REFERENCES silver.securities(id),
    weight          double precision NOT NULL,
    target_holding_days integer,
    rationale_id    bigint REFERENCES silver.predictions(id),
    PRIMARY KEY (portfolio_id, asof_date, security_id)
);

CREATE TABLE silver.risk_events (
    id              bigserial PRIMARY KEY,
    portfolio_id    bigint REFERENCES silver.portfolios(id),
    event_type      text NOT NULL,           -- drawdown_halt, capacity_breach, position_cap_hit, etc.
    severity        text NOT NULL,           -- info, warning, halt
    occurred_at     timestamptz NOT NULL DEFAULT now(),
    details         jsonb,
    resolved_at     timestamptz
);
```

### 7.13 Data quality

```sql
CREATE TABLE silver.data_quality_findings (
    id              bigserial PRIMARY KEY,
    finding_type    text NOT NULL,
    severity        text NOT NULL CHECK (severity IN ('info', 'warning', 'error')),
    security_id     bigint REFERENCES silver.securities(id),
    vendor          text,
    period_start    date,
    period_end      date,
    summary         text NOT NULL,
    evidence        jsonb,
    blocks_predictions boolean NOT NULL DEFAULT false,
    discovered_at   timestamptz NOT NULL DEFAULT now(),
    resolved_at     timestamptz,
    resolution_note text
);
```

---

## 8. Feature Store Contract

### 8.1 Numeric features

Computed deterministically from `prices_daily` + `fundamental_facts` + `corporate_actions`. Pure SQL or pandas. No randomness, no external API calls.

Examples:
- `momentum_12_1` — return from t-252d to t-21d, excluding most recent 21 days
- `avg_dollar_volume_63` — average `adj_close * volume` over trailing 63 visible
  daily price rows
- `realized_volatility_63` — annualized sample stdev of daily returns over
  trailing 63 visible daily price rows
- `volatility_30d` — annualized stdev of daily log returns over trailing 30 days
- `revenue_growth_yoy` — TTM revenue / prior-TTM revenue minus 1
- `gross_margin_change_yoy` — current TTM gross margin minus prior TTM gross margin
- `ev_to_revenue_sector_rank` — ratio's sector-relative rank
- `roic` — NOPAT / average invested capital, computed from facts
- `sector_relative_momentum` — `momentum_12_1 - sector_median_momentum_12_1`

Versioning: changes in computation logic increment `feature_definitions.version`. Two versions of the same feature can coexist; backtests reference exact `(name, version)` tuple.

### 8.2 Text features

Computed once per artifact via LLM at artifact ingest time. Cached immutably in `feature_values` keyed by `(security_id, asof_date, feature_definition_id)` where `asof_date = artifact.available_at::date`.

Examples (initial set):
- `management_confidence` — 0.0–1.0 confidence score from earnings call prepared remarks
- `cfo_hedging_intensity` — count of hedging phrases per 1000 words in CFO segments
- `forward_guidance_strength` — categorical encoded numerically: 0=none, 1=withdrawn, 2=implicit, 3=explicit_qualitative, 4=explicit_quantitative
- `risk_factor_yoy_delta` — Jaccard distance of risk-factor section vs prior year
- `demand_language_shift` — sentiment delta on demand-related sentences quarter-over-quarter

Versioning: changing the LLM model or prompt creates a NEW `feature_definitions` row with a new version. Old feature values remain unchanged. Old backtests remain reproducible.

### 8.3 Feature snapshots

A feature snapshot is a frozen materialization of feature values for a specific feature set across a date range. Used so backtests reference an immutable input — re-running the same backtest pulls the same snapshot.

Snapshot creation: enumerate the feature_definition_ids you want, hash them, write to `feature_snapshots`, then materialize values (pre-computing if needed). Subsequent backtests reference `feature_snapshot_id`.

### 8.4 Feature value invariants

For any `feature_value` row:
- `asof_date >= source_artifact.available_at::date` (PIT)
- `asof_date >= source_event.available_at::date` (PIT)
- The computation is reproducible: given `(security_id, asof_date, feature_definition_id)` and the underlying data at the time of computation, the value is byte-identical.

---

## 9. Labels Contract

### 9.1 Forward returns

For each (security_id, label_date, horizon_days):
- Find the trading day exactly `horizon_days` after `label_date` (handling weekends, holidays, early closes via `trading_calendar`)
- Compute log return using split- and dividend-adjusted close prices
- For excess returns, subtract benchmark log return over the same window

### 9.2 Target kinds

- `raw_return` — security log return over horizon
- `excess_return_market` — security minus market index (default SPY)
- `excess_return_sector` — security minus sector ETF (e.g., XLK for tech)
- `risk_adjusted_return` — `(raw_return - risk_free) / trailing_volatility`

Default prediction target: `excess_return_market`. Predicting raw return reduces to predicting market direction, which is not the goal.

### 9.3 Label availability

Labels are written when their horizon elapses. A 63-trading-day label for
`label_date = 2026-04-28` becomes available only after the 63rd later trading
day has an adjusted close price. Predictions made on `2026-04-28` cannot be
scored until then. The system explicitly does not score predictions before
their labels exist.

---

## 10. Backtest Engine Contract

### 10.1 Inputs

- `feature_snapshot_ref` — frozen feature-set reference, usually a feature snapshot id once snapshots exist
- `model_definition` — algorithm + hyperparameters (e.g., ridge, gradient-boosted trees, simple z-score ensemble)
- `training_start_date` / `training_end_date` — training window dates (typically 252 trading days rolling)
- `test_start_date` / `test_end_date` — test window dates (typically 1 trading month rolling)
- `horizon_days` — prediction horizon
- `target_kind` — what kind of label
- `universe_name` — which point-in-time universe
- `cost_assumptions` — execution-assumption set, including cost model inputs
- `parameters` — model and harness parameters
- `available_at_policy_versions` — map of source/policy names to immutable versions
- `input_fingerprints` — stable hashes for frozen input sets used by the run
- `random_seed` — for reproducibility

### 10.2 Outputs

A `backtest_run` row with `metrics` jsonb containing:

```json
{
  "n_predictions": 12000,
  "ic_mean": 0.04,
  "ic_std": 0.18,
  "ic_t_stat": 2.41,
  "sharpe_long_short": 0.78,
  "sharpe_long_only": 0.65,
  "sharpe_net_of_costs": 0.41,
  "hit_rate": 0.535,
  "max_drawdown": -0.12,
  "drawdown_recovery_days": 87,
  "turnover_annual": 4.2,
  "avg_position_count": 8,
  "capacity_estimate_usd": 25000000,
  "factor_exposures": {"market": 0.05, "size": -0.12, "value": 0.08, "momentum": 0.31}
}
```

Plus `metrics_by_regime`:
```json
{
  "pre_2019": {"sharpe_net": 0.55, "n": 3000},
  "2020_dislocation": {"sharpe_net": 0.18, "n": 800},
  "2021_2023_rates": {"sharpe_net": 0.42, "n": 4200},
  "2024_plus_ai": {"sharpe_net": 0.39, "n": 4000}
}
```

### 10.3 Required tests per backtest

Every backtest run must include:

1. **Walk-forward validation** — train on rolling 252-day window, predict 1-month forward, no peeking
2. **Baseline comparison** — at minimum, compare against random portfolio, equal-weight, and 12-1 momentum
3. **Net-of-cost reporting** — gross AND net Sharpe; never gross alone
4. **Regime breakdown** — metrics across 4 regimes minimum (pre-2019, 2020, 2021-23, 2024+)
5. **Label-scramble test** — same model on permuted labels must produce Sharpe near zero. If it doesn't, the model or harness is broken.
6. **Multiple-comparisons correction** — when testing N hypotheses, Benjamini-Hochberg at α=0.05 against the family

Falsifier markdown reports are user-facing evidence artifacts, not just links
to registry rows. A complete falsifier report must include:

- status and an explicit no-alpha-claim statement for non-accepted evidence
- normalized run config, report path, and exact command
- data coverage for joined feature/label rows, distinct tickers, distinct
  as-of dates, as-of range, and horizon range
- point-in-time universe membership used by the run
- `model_run_id`, `model_run_key`, `backtest_run_id`, and `backtest_run_key`
- git SHA, feature definition hash, feature set hash, joined input fingerprint,
  available-at policy versions, random seed, target kind, and execution
  assumptions
- model training/test windows and their source
- gross and net headline metrics, baseline comparison, and cost assumptions
- regime evidence with regime names, date ranges, sample counts, strategy net
  returns, baseline net returns, and net differences
- label-scramble evidence with the scored-row source, selection rule, seed,
  trial count, alpha, observed score, null summary, p-value, and pass/fail
  result, or a deterministic insufficiency/failure reason
- multiple-comparisons setting used for the claim family
- traceability validation result showing the report agrees with the joined
  `model_runs` and `backtest_runs` metadata before the artifact is written

### 10.4 Cost model (default)

Encoded in `execution_assumptions`. Default values:

- Half-spread: 5 bps for liquid (>$100M ADV), 15 bps for mid (>$10M ADV), 40 bps for small
- Market impact: square-root model, `impact_bps = 10 × sqrt(trade_value / ADV)` capped at 50 bps
- Borrow cost on shorts: 25 bps annualized for liquid, higher for hard-to-borrow (manual flag)
- Fill convention: `next_open` (predictions made at close, executed at next open)
- Minimum liquidity filter: $10M ADV

### 10.5 Capacity estimate

Computed as: maximum AUM at which the strategy could deploy without market impact eating more than 20% of expected return. Formula:
```
capacity = min over all positions of (
    target_weight × AUM × turnover_implied_share_of_ADV ≤ 0.05 × ADV
)
```

A strategy with Sharpe 1.5 and capacity $500K is irrelevant.

---

## 11. AI Layer Contract

### 11.1 The three jobs

The LLM does exactly three things:

1. **Extract structured features from text.** Input: artifact text. Output: numeric feature value + confidence. Versioned by (model_version, prompt_version).
2. **Propose hypotheses.** Input: recent backtest results, unexplored data corners. Output: structured hypothesis definitions.
3. **Explain validated results.** Input: a hypothesis that passed validation. Output: prose explanation of why it might work, in service of human understanding. **Post-hoc only.**

### 11.2 What the LLM never does

- Compute returns or any other math
- Write to `labels`, `predictions`, `outcomes`, or any backtest result table
- Decide whether a hypothesis is validated (statistics decides)
- Override risk controls
- Synthesize features from features (composition is deterministic SQL)
- Re-validate its own proposals (the adversarial critic is a separate model run)

### 11.3 Model routing

Three tiers, escalation on quality-gate failure:

| Tier | Model class | Use case | Cost (per M output tokens) |
|---|---|---|---|
| 1 | Local Llama 3.1 8B (Ollama) | Sentiment, classification, structured extraction | $0 marginal |
| 2 | Open-source 70B (Together / Groq) | Comparative reasoning, language shift detection | ~$0.88 |
| 3 | Frontier (Claude Sonnet, GPT-4o) | Hypothesis generation, post-hoc explanation, hard synthesis | ~$15 |

Quality gate between tiers: structured output validates against schema, confidence above threshold, no obvious hallucination patterns. Failure escalates.

### 11.4 Cost / latency budget

- Daily incremental feature extraction (only new artifacts): target <30 min total wall clock at 50-ticker scale, <2 hours at 500-ticker scale
- Hypothesis generation: weekly batch, <5 min
- Adversarial critic: per candidate hypothesis, <30 sec
- Total monthly LLM spend at falsifier scale: <$50

---

## 12. Hypothesis Lifecycle

```
                     ┌──────────────┐
   LLM proposes ────►│  candidate   │
                     └──────┬───────┘
                            │
                     adversarial critic
                            │
                ┌───────────┴───────────┐
                │                       │
                ▼                       ▼
          ┌──────────┐            ┌──────────┐
          │  killed  │            │ proceed  │
          └──────────┘            └─────┬────┘
                                        │
                            backtest validation
                            (walk-forward + regime
                             + label-scramble +
                             multiple-comparisons)
                                        │
                            ┌───────────┴───────────┐
                            │                       │
                            ▼                       ▼
                      ┌──────────┐           ┌────────────┐
                      │ rejected │           │ validated  │
                      └──────────┘           └─────┬──────┘
                                                   │
                                       3 months OOS performance
                                                   │
                                       ┌───────────┴───────────┐
                                       │                       │
                                       ▼                       ▼
                                 ┌──────────┐           ┌──────────┐
                                 │  retired │           │   live   │
                                 └──────────┘           └─────┬────┘
                                                              │
                                                  continuous monitoring
                                                              │
                                                              ▼
                                                  drift detection,
                                                  decay, retire if
                                                  signal dies
```

### Promotion gates

**candidate → validated:** all of these must hold:
- In-sample Sharpe > 0.5
- Out-of-sample Sharpe > 0.3 over a held-out year
- Survives label-scramble test (scrambled labels yield near-zero Sharpe)
- Survives multiple-comparisons correction
- Adversarial critic finds no PIT violation
- Capacity > $1M
- Decay curve doesn't crash to zero in <30 days

Feature Candidate Pack v1 is an early manual candidate stage, not a validation
claim. It runs a configured family of numeric hypotheses from
`config/feature_candidates.yaml` through the same falsifier and registry path
so failed ideas are visible instead of forgotten. The first configured family
includes adjusted-close return windows, liquidity, and realized-volatility
candidates. Family-level multiple-comparisons correction remains a required
promotion gate before any candidate moves beyond exploratory evidence.

Harder Falsifier v1 adds a walk-forward consistency rollup over each configured
candidate. The falsifier must persist split-level `walk_forward_windows` in the
durable model/backtest metrics, and the pack-level runner must reject candidates
that do not beat the equal-weight baseline across enough windows even if their
aggregate headline metric looks favorable. Label-scramble remains a separate
required gate.

**validated → live:** at least 3 months of paper-trading performance consistent with backtest expectation (within 30% of expected Sharpe).

**live → retired:** performance drift exceeds threshold (e.g., 90-day rolling Sharpe < 50% of backtest Sharpe), OR signal IC turns negative, OR capacity falls below threshold.

---

## 13. Portfolio Layer Contract

### 13.1 Sim portfolio (backtest-only)

- Built inside backtest_runs from predictions
- Trade execution at end-of-period close or next-period open per `execution_assumptions`
- Costs and impact applied per trade
- Used to compute Sharpe-net-of-costs, drawdown, capacity

### 13.2 Paper portfolio (forward live)

- Real-time, daily updates
- Predictions generated at market close → portfolio rebalanced at next open
- Either internal ledger (synthetic) or broker paper account API (e.g., Alpaca)
- Tracks: position weights, realized fills, slippage, actual borrow rates
- Compared monthly against backtest expectation; meaningful drift triggers `risk_event`

### 13.3 Risk controls (default)

- Max single position: 5% of portfolio
- Max sector exposure: 30%
- Max gross exposure (long + short): 200% of NAV
- Max turnover: 500% annualized
- Liquidity filter: only trade names with >$10M ADV
- Drawdown halt: if rolling 30-day return < -10%, write `risk_event` and pause new entries
- Concentration cap: top 10 positions ≤ 50% of portfolio

### 13.4 Capacity gates

If estimated capacity falls below $5M, portfolio enters degraded mode (smaller positions, longer holding periods). If below $1M, halt entirely.

---

## 14. Runtime Jobs

### 14.1 Daily job (post-close, ~1 hour)

1. Ingest new vendor data: prices, news, any filings released today
2. Apply PIT normalization → events, artifacts, facts
3. Compute new features for newly-arrived artifacts (numeric + text)
4. Score open predictions whose horizons elapsed today
5. Generate today's predictions for all live hypotheses
6. Update paper portfolio (target positions for next-open execution)
7. Run risk control checks; emit risk_events if triggered
8. Write daily report to `reports/daily/YYYY-MM-DD.md`

### 14.2 Weekly job (Monday morning)

1. Evaluate hypothesis drift (live performance vs backtest expectation)
2. Retire hypotheses below threshold
3. Adversarial critic reviews live hypotheses
4. LLM proposes 5–10 new candidate hypotheses
5. Critic evaluates candidates → kill or proceed
6. Write weekly report

### 14.3 Monthly job (first Monday)

1. Full backtest rerun with latest data
2. Compare live performance vs backtest over past month
3. Promote validated hypotheses to live (if eligible)
4. Calibration analysis (does 70% confidence call hit 70%?)
5. Capacity and factor-exposure refresh
6. Cost / spend analysis: LLM costs, vendor costs

### 14.4 Quarterly job (manual review)

Operator review of:
- Universe changes (additions, delistings)
- Available_at policy changes
- Model selection decisions
- Whether thesis is still alive

---

## 15. Reproducibility Contract

Every result row carries enough metadata to rebuild it from source:

| Result type | Required metadata |
|---|---|
| `fundamental_facts` row | `raw_object_id`, `available_at_policy_id`, `normalization_version` |
| `feature_values` row | `feature_definition_id` (which encodes `model_version` + `prompt_version`), `source_event_id` or `source_artifact_id`, `computed_at` |
| `predictions` row | `model_run_id` (which encodes `code_git_sha`, `feature_set_hash`, `feature_snapshot_ref`, training/test date windows, `random_seed`, `cost_assumptions`, `parameters`, `available_at_policy_versions`, and `input_fingerprints`) |
| `backtest_runs` row | All of the model-run metadata by `model_run_id` plus `backtest_run_key`, `universe_name`, `cost_assumptions`, `metrics`, `metrics_by_regime`, `baseline_metrics`, `label_scramble_metrics`, `label_scramble_pass`, `multiple_comparisons_correction`, and final `status` |

Accepted backtest claims are traced from the reported `backtest_run_key` or
`backtest_run_id` to `backtest_runs`, then across the durable
`backtest_runs.model_run_id` join to `model_runs`. A report that cannot resolve
that join, or whose report metadata disagrees with the joined registry rows, is
not reproducible and must not be used as evidence for the thesis.

Replay procedure:
1. Resolve `backtest_run_id` or `backtest_run_key` to one `backtest_runs` row.
2. Resolve `backtest_runs.model_run_id` to one `model_runs` row.
3. Reject the replay if the row, join, terminal accepted-claim status, or
   required replay inputs are missing.
4. Check out `code_git_sha`.
5. Apply the recorded `available_at_policy_versions`; do not use whatever
   policy is current at replay time.
6. Materialize features from `feature_snapshot_ref` when present, otherwise
   verify the frozen persisted inputs from `input_fingerprints`.
7. Train with `random_seed` over `training_start_date` through
   `training_end_date`.
8. Predict over `test_start_date` through `test_end_date`.
9. Recompute deterministic model/backtest keys and report-critical metrics.
10. Reject on any field mismatch; otherwise verify byte-identical predictions
    and matching accepted-claim evidence.

---

## 16. Repository Structure

```
~/Silver/
├── README.md                       (mission, invariants, contributing)
├── SPEC.md                         (this document)
├── pyproject.toml
├── .env.example
├── .gitignore
│
├── config/
│   ├── universe.yaml               (initial ticker list, tier definitions)
│   ├── available_at_policies.yaml  (default policy versions)
│   ├── execution_assumptions.yaml  (cost models)
│   └── llm_router.yaml             (model tier assignments)
│
├── db/
│   ├── migrations/
│   │   ├── 001_foundation.sql
│   │   ├── 002_normalization.sql
│   │   ├── 003_features_and_labels.sql
│   │   ├── 004_models_and_predictions.sql
│   │   ├── 005_portfolios.sql
│   │   └── 006_hypotheses.sql
│   └── seed/
│       ├── trading_calendar.csv    (10y of NYSE calendar)
│       └── universe_seed.yaml
│
├── src/silver/
│   ├── __init__.py
│   ├── time/                       (asof_date utilities, calendar, available_at)
│   ├── data/                       (db connection, query helpers)
│   ├── ingest/                     (FMP, SEC, news clients + raw vault)
│   ├── normalize/                  (raw → events, artifacts, facts, prices)
│   ├── features/
│   │   ├── numeric/
│   │   ├── text/                   (LLM-driven feature extractors)
│   │   └── store.py
│   ├── labels/                     (forward return computation)
│   ├── models/                     (algorithm wrappers: ridge, gbt, etc.)
│   ├── backtest/
│   │   ├── walk_forward.py
│   │   ├── costs.py
│   │   ├── regimes.py
│   │   └── tests.py                (label-scramble, multiple-comparisons)
│   ├── hypotheses/
│   │   ├── generator.py            (LLM-based)
│   │   ├── critic.py               (adversarial review)
│   │   ├── validator.py            (backtest gate)
│   │   └── lifecycle.py
│   ├── portfolio/
│   │   ├── construct.py
│   │   ├── execute_sim.py
│   │   ├── execute_paper.py
│   │   └── risk_controls.py
│   ├── llm/
│   │   ├── router.py
│   │   ├── prompts/                (versioned prompt files)
│   │   └── providers/              (anthropic, together, ollama)
│   ├── runtime/
│   │   ├── daily.py
│   │   ├── weekly.py
│   │   └── monthly.py
│   └── reports/                    (daily/weekly/monthly markdown generators)
│
├── scripts/
│   ├── apply_migrations.py
│   ├── seed_universe.py
│   ├── ingest_prices.py
│   ├── ingest_fundamentals.py
│   ├── ingest_transcripts.py
│   ├── ingest_filings.py
│   ├── compute_features.py
│   ├── run_backtest.py
│   ├── run_falsifier.py
│   └── run_daily.py
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/                   (small reproducible data fixtures)
│
└── reports/                        (generated artifacts, gitignored beyond samples)
    ├── daily/
    ├── weekly/
    ├── monthly/
    └── falsifier/
```

---

## 17. Build Plan

### Phase 1 — Foundation (1.5 weeks)

**Goal:** Silver can persist data with full PIT discipline and reproduce a known anomaly on a small cross-sector universe.

Deliverables:
- Migration `001_foundation.sql` applied
- Trading calendar seeded for 2014–2026
- 45 initial liquid securities seeded across major non-REIT sectors
- Universe `falsifier_seed` populated
- `available_at_policies` table populated with initial policy versions
- FMP + SEC clients with rate limits
- Raw vault writer with idempotent storage
- Prices ingested for 45 tickers × 10 years
- Forward labels computed
- Walk-forward harness runnable
- **12-1 momentum anomaly reproduced on the 45-ticker falsifier universe with positive Sharpe**

**Phase 1 exit criterion:** A `pytest tests/integration/test_momentum_replication.py` passes, asserting that 12-1 momentum has Sharpe > 0.2 on the 40-50 ticker / 10-year universe under realistic costs.

### Phase 2 — Trustworthy backtest infrastructure (1.5 weeks)

**Goal:** Every backtest result is reproducible, regime-broken, label-scramble-tested, and net-of-costs.

Deliverables:
- Migration `002_normalization.sql` and `003_features_and_labels.sql` applied
- `model_runs` and `backtest_runs` tables with full metadata
- Cost model implementation (`execution_assumptions`)
- Regime split logic
- Label-scramble test
- Multiple-comparisons correction
- Capacity / drawdown / factor-exposure metrics
- Reproducibility test: run same backtest twice → identical `backtest_run` metrics

**Phase 2 exit criterion:** A backtest can be reproduced byte-identically from its `model_run_id` alone.

### Phase 3 — Numeric features and baseline models (1 week)

**Goal:** A numeric-only model produces meaningful Sharpe and serves as the bar AI must beat.

Deliverables:
- 12–15 deterministic numeric features
- 5 baseline models scored on the falsifier universe
- Numeric ensemble model (linear regression of all numeric features)
- Per-feature backtest reports
- Feature snapshot infrastructure

**Phase 3 exit criterion:** Numeric ensemble reports a stable, positive net-of-costs Sharpe across regimes on the falsifier universe; results survive label-scramble.

### Phase 4 — AI text features (2 weeks)

**Goal:** First 5–10 AI-extracted text features, computed immutably with full versioning.

Deliverables:
- LLM model router with three tiers
- Versioned prompt files
- Transcript ingest pipeline
- Filing ingest pipeline
- 5–10 text feature extractors
- Bulk computation over historical artifacts
- Feature values populated for falsifier universe

**Phase 4 exit criterion:** Text features computable end-to-end; running the same extractor twice produces identical values.

### Phase 5 — The falsifier verdict (3 days)

**Goal:** Definitive answer: do AI text features improve over numeric-only?

Deliverables:
- Numeric-only ensemble vs numeric+text ensemble backtest
- Multiple-comparisons-corrected significance test
- Per-regime breakdown
- Adversarial critic review of any positive result
- One reproducible report at `reports/falsifier/v1.md`

**Phase 5 decision rule:**
- Numeric+text ensemble Sharpe > numeric ensemble Sharpe by ≥ 0.2 net of costs, surviving correction → thesis lives, proceed to Phase 6
- No improvement → thesis fails at this scale; document and either iterate text features or abandon

### Phase 6 — Predictions, paper portfolio, outcomes (1 week)

**Goal:** Live operational system that makes daily predictions and scores them.

Deliverables:
- Daily prediction generation for live hypotheses
- Paper portfolio (internal ledger first; broker integration optional)
- Outcome scoring as labels become available
- Daily report generator
- Risk controls operational

**Phase 6 exit criterion:** Daily job runs unattended for 1 week, producing daily reports and updating paper portfolio.

### Phase 7 — Hypothesis machine (1 week)

**Goal:** AI proposes hypotheses, critic attacks, backtest decides; lifecycle automated.

Deliverables:
- Hypothesis generator (LLM-based)
- Adversarial critic
- Validator (full backtest gate)
- Lifecycle state machine
- Weekly job operational

**Phase 7 exit criterion:** Weekly job autonomously proposes, critiques, validates, and either kills or promotes hypotheses. At least 1 candidate survives the full pipeline (or gets correctly killed).

### Total timeline

~10 weeks to phase 7 complete.
~7 weeks to falsifier verdict (phase 5).

If Phase 5 fails, the system is still useful: a clean numeric quant infrastructure with PIT discipline. Iteration on text features can continue.

---

## 18. First Milestone (Week 1 Deliverable)

The most concrete possible week-1 target:

**Day 1–2:**
- Repo bootstrap, virtualenv, pyproject
- Migration 001 applied
- 45 securities seeded
- 10y NYSE calendar seeded

**Day 3–4:**
- FMP client built and tested
- Prices ingested for 45 tickers × 10 years (~113,000 rows)
- Forward labels computed at 5/21/63/126/252-trading-day horizons

**Day 5:**
- Walk-forward backtest harness skeleton
- 12-1 momentum feature computed
- One backtest run completed end-to-end with model_run_id and metrics

**Day 6–7:**
- Cost model applied
- Multiple regimes split out
- Label-scramble test passing
- First report at `reports/falsifier/week_1_momentum.md`
- Test suite green

**Week 1 success:** A repeatable command `python scripts/run_falsifier.py --strategy momentum_12_1 --horizon 63 --universe falsifier_seed` produces a complete backtest report whose Sharpe is reproducible across runs and consistent with academic literature on 12-1 momentum.

---

## 19. Open Decisions Before Phase 1

These need answers before phase 1 begins. None of them block reading this spec.

| Decision | Default recommendation |
|---|---|
| Postgres only or Postgres + DuckDB hybrid? | Postgres only for v1. Add DuckDB at phase 5+ if feature-store queries get slow. |
| Survivorship-bias source | Accept current-S&P universe for falsifier; document as known limitation; revisit before scaling beyond 50 tickers. |
| Trading calendar source | `pandas_market_calendars` library |
| Initial benchmark for excess returns | SPY (market) and XLK/XLF/etc. (sector) |
| Local LLM stack | Ollama for ease; switch to vLLM if throughput becomes a bottleneck |
| Frontier LLM provider | Claude Sonnet via Anthropic API |
| Mid-tier LLM provider | Together AI for Llama 70B |
| News vendor | Skip until phase 2+ |
| Broker paper account | Internal ledger first; consider Alpaca for phase 6 if paper feel matters |

---

## 20. Risk Register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| PIT violation creates lookahead bias | Medium | Catastrophic (invalidates all results) | Encoded `available_at` policy table; specific tests assert PIT discipline; adversarial critic looks for violations |
| Feature versioning breaks reproducibility | Medium | High | Immutable feature values keyed by feature_definition version; prompt/model versions recorded explicitly |
| Backtest overfits via multiple comparisons | High | High | Benjamini-Hochberg correction; out-of-sample holdout; label-scramble required |
| AI text features look great in backtest, fail live | Medium | Medium | 3-month paper validation required before live; drift monitoring continuous |
| LLM cost runs away | Low | Medium | Model router prefers cheap tiers; daily incremental only on new artifacts; monthly cost ceiling |
| Vendor data gaps create silent holes | Medium | Medium | `data_quality_findings` writes; predictions can be blocked on gaps; weekly review |
| Survivorship bias inflates backtest performance | High at scale | High | Documented for falsifier (40-50 tickers); must address before universe expansion |
| Regime change kills validated hypothesis | Medium | Medium | Per-regime metrics in every backtest; live drift monitoring; auto-retirement |
| Capacity invisible at scale | Medium | High | Capacity estimate in every backtest; capacity gates in portfolio; halt below threshold |
| Code drift breaks reproducibility | Low | Catastrophic | Every result tagged with code_git_sha; replay procedure documented |

---

## 21. Out of Scope (Restated)

- Live capital deployment of any size
- Reinforcement learning
- Options, futures, fixed income, foreign equities, crypto
- Intraday signals
- Real-time market microstructure
- Multi-user collaboration / sharing
- Web UI before phase 6
- Manual analyst override workflows
- Macro factor models beyond manual era splits
- Sector rotation strategies (initially)
- ESG signals
- Alternative data (satellite, web traffic, credit card spend)

These can become scope in a v2 if the v1 thesis survives.

---

## 22. Bottom Line

Silver is a falsification machine for one thesis: AI-extracted text features improve forward-return prediction in US equities net of costs. It is built native, reproducible, and adversarially tested from day one. The first ten weeks produce a verdict; the system is engineered to admit "no edge" as a valid outcome.

Build accordingly.

---

*This document is the canonical spec. Save as `~/Silver/SPEC.md`. Subsequent build phases reference its sections by number. Changes to the spec require a versioned amendment with rationale.*
