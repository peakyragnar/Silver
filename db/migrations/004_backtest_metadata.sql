CREATE TABLE silver.model_runs (
    id bigserial PRIMARY KEY,
    model_run_key text NOT NULL,
    name text NOT NULL,
    code_git_sha text NOT NULL,
    feature_set_hash text NOT NULL,
    feature_snapshot_ref text,
    training_start_date date NOT NULL,
    training_end_date date NOT NULL,
    test_start_date date NOT NULL,
    test_end_date date NOT NULL,
    horizon_days integer NOT NULL,
    target_kind text NOT NULL,
    random_seed integer NOT NULL,
    cost_assumptions jsonb NOT NULL DEFAULT '{}'::jsonb,
    parameters jsonb NOT NULL DEFAULT '{}'::jsonb,
    metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    available_at_policy_versions jsonb NOT NULL DEFAULT '{}'::jsonb,
    input_fingerprints jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status text NOT NULL DEFAULT 'running',
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (model_run_key),
    CHECK (btrim(model_run_key) <> ''),
    CHECK (btrim(name) <> ''),
    CHECK (code_git_sha ~ '^[0-9a-f]{7,64}$'),
    CHECK (feature_set_hash ~ '^[0-9a-f]{64}$'),
    CHECK (feature_snapshot_ref IS NULL OR btrim(feature_snapshot_ref) <> ''),
    CHECK (training_end_date >= training_start_date),
    CHECK (test_end_date >= test_start_date),
    CHECK (test_start_date > training_end_date),
    CHECK (horizon_days IN (5, 21, 63, 126, 252)),
    CHECK (
        target_kind IN (
            'raw_return',
            'excess_return',
            'excess_return_market',
            'excess_return_sector',
            'risk_adjusted_return'
        )
    ),
    CHECK (random_seed >= 0),
    CHECK (jsonb_typeof(cost_assumptions) = 'object'),
    CHECK (jsonb_typeof(parameters) = 'object'),
    CHECK (jsonb_typeof(metrics) = 'object'),
    CHECK (jsonb_typeof(available_at_policy_versions) = 'object'),
    CHECK (jsonb_typeof(input_fingerprints) = 'object'),
    CHECK (finished_at IS NULL OR finished_at >= started_at),
    CHECK (status IN ('running', 'succeeded', 'failed', 'insufficient_data')),
    CHECK ((status = 'running') = (finished_at IS NULL)),
    CHECK (status <> 'succeeded' OR metrics <> '{}'::jsonb)
);

CREATE INDEX model_runs_status_started_idx
    ON silver.model_runs (status, started_at);

CREATE INDEX model_runs_target_test_window_idx
    ON silver.model_runs (horizon_days, target_kind, test_start_date, test_end_date);

CREATE TABLE silver.backtest_runs (
    id bigserial PRIMARY KEY,
    backtest_run_key text NOT NULL,
    model_run_id bigint NOT NULL REFERENCES silver.model_runs(id) ON DELETE RESTRICT,
    name text NOT NULL,
    universe_name text NOT NULL,
    horizon_days integer NOT NULL,
    target_kind text NOT NULL,
    cost_assumptions jsonb NOT NULL DEFAULT '{}'::jsonb,
    parameters jsonb NOT NULL DEFAULT '{}'::jsonb,
    metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    metrics_by_regime jsonb NOT NULL DEFAULT '{}'::jsonb,
    baseline_metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    label_scramble_metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    label_scramble_pass boolean,
    multiple_comparisons_correction text,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status text NOT NULL DEFAULT 'running',
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (backtest_run_key),
    CHECK (btrim(backtest_run_key) <> ''),
    CHECK (btrim(name) <> ''),
    CHECK (btrim(universe_name) <> ''),
    CHECK (horizon_days IN (5, 21, 63, 126, 252)),
    CHECK (
        target_kind IN (
            'raw_return',
            'excess_return',
            'excess_return_market',
            'excess_return_sector',
            'risk_adjusted_return'
        )
    ),
    CHECK (jsonb_typeof(cost_assumptions) = 'object'),
    CHECK (jsonb_typeof(parameters) = 'object'),
    CHECK (jsonb_typeof(metrics) = 'object'),
    CHECK (jsonb_typeof(metrics_by_regime) = 'object'),
    CHECK (jsonb_typeof(baseline_metrics) = 'object'),
    CHECK (jsonb_typeof(label_scramble_metrics) = 'object'),
    CHECK (
        multiple_comparisons_correction IS NULL
        OR multiple_comparisons_correction IN ('bh', 'bonferroni', 'none')
    ),
    CHECK (finished_at IS NULL OR finished_at >= started_at),
    CHECK (status IN ('running', 'succeeded', 'failed', 'insufficient_data')),
    CHECK ((status = 'running') = (finished_at IS NULL)),
    CHECK (status <> 'succeeded' OR metrics <> '{}'::jsonb),
    CHECK (
        status NOT IN ('succeeded', 'insufficient_data')
        OR label_scramble_pass IS NOT NULL
    )
);

CREATE INDEX backtest_runs_model_run_idx
    ON silver.backtest_runs (model_run_id);

CREATE INDEX backtest_runs_universe_target_started_idx
    ON silver.backtest_runs (
        universe_name,
        horizon_days,
        target_kind,
        started_at
    );
