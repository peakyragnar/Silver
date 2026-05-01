CREATE TABLE silver.hypotheses (
    id bigserial PRIMARY KEY,
    hypothesis_key text NOT NULL,
    name text NOT NULL,
    thesis text NOT NULL,
    signal_name text NOT NULL,
    mechanism text NOT NULL,
    universe_name text,
    horizon_days integer,
    target_kind text,
    status text NOT NULL DEFAULT 'proposed',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (hypothesis_key),
    CHECK (btrim(hypothesis_key) <> ''),
    CHECK (btrim(name) <> ''),
    CHECK (btrim(thesis) <> ''),
    CHECK (btrim(signal_name) <> ''),
    CHECK (btrim(mechanism) <> ''),
    CHECK (universe_name IS NULL OR btrim(universe_name) <> ''),
    CHECK (horizon_days IS NULL OR horizon_days IN (5, 21, 63, 126, 252)),
    CHECK (
        target_kind IS NULL
        OR target_kind IN (
            'raw_return',
            'excess_return',
            'excess_return_market',
            'excess_return_sector',
            'risk_adjusted_return'
        )
    ),
    CHECK (
        status IN (
            'proposed',
            'running',
            'rejected',
            'promising',
            'accepted',
            'retired'
        )
    ),
    CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX hypotheses_status_idx
    ON silver.hypotheses (status, created_at);

CREATE INDEX hypotheses_signal_idx
    ON silver.hypotheses (signal_name, universe_name, horizon_days);

CREATE TABLE silver.hypothesis_evaluations (
    id bigserial PRIMARY KEY,
    hypothesis_id bigint NOT NULL REFERENCES silver.hypotheses(id) ON DELETE RESTRICT,
    model_run_id bigint NOT NULL REFERENCES silver.model_runs(id) ON DELETE RESTRICT,
    backtest_run_id bigint NOT NULL REFERENCES silver.backtest_runs(id) ON DELETE RESTRICT,
    evaluation_status text NOT NULL,
    failure_reason text,
    notes text,
    summary_metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (hypothesis_id, backtest_run_id),
    CHECK (
        evaluation_status IN (
            'running',
            'rejected',
            'promising',
            'accepted',
            'failed'
        )
    ),
    CHECK (failure_reason IS NULL OR btrim(failure_reason) <> ''),
    CHECK (notes IS NULL OR btrim(notes) <> ''),
    CHECK (jsonb_typeof(summary_metrics) = 'object')
);

CREATE INDEX hypothesis_evaluations_hypothesis_created_idx
    ON silver.hypothesis_evaluations (hypothesis_id, created_at DESC, id DESC);

CREATE INDEX hypothesis_evaluations_backtest_idx
    ON silver.hypothesis_evaluations (backtest_run_id);
