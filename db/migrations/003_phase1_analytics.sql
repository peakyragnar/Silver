CREATE TABLE silver.analytics_runs (
    id bigserial PRIMARY KEY,
    run_kind text NOT NULL,
    code_git_sha text NOT NULL,
    feature_set_hash text,
    available_at_policy_versions jsonb NOT NULL DEFAULT '{}'::jsonb,
    parameters jsonb NOT NULL DEFAULT '{}'::jsonb,
    input_fingerprints jsonb NOT NULL DEFAULT '{}'::jsonb,
    random_seed integer,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status text NOT NULL DEFAULT 'running',
    CHECK (
        run_kind IN (
            'price_normalization',
            'label_generation',
            'feature_generation',
            'backtest'
        )
    ),
    CHECK (btrim(code_git_sha) <> ''),
    CHECK (feature_set_hash IS NULL OR btrim(feature_set_hash) <> ''),
    CHECK (jsonb_typeof(available_at_policy_versions) = 'object'),
    CHECK (jsonb_typeof(parameters) = 'object'),
    CHECK (jsonb_typeof(input_fingerprints) = 'object'),
    CHECK (finished_at IS NULL OR finished_at >= started_at),
    CHECK (status IN ('running', 'succeeded', 'failed'))
);

CREATE INDEX analytics_runs_kind_started_idx
    ON silver.analytics_runs (run_kind, started_at);

CREATE TABLE silver.prices_daily (
    security_id bigint NOT NULL REFERENCES silver.securities(id),
    date date NOT NULL REFERENCES silver.trading_calendar(date),
    open numeric(18, 6) NOT NULL,
    high numeric(18, 6) NOT NULL,
    low numeric(18, 6) NOT NULL,
    close numeric(18, 6) NOT NULL,
    adj_close numeric(18, 6) NOT NULL,
    volume bigint NOT NULL,
    currency text NOT NULL DEFAULT 'USD',
    source_system text NOT NULL,
    normalization_version text NOT NULL,
    available_at timestamptz NOT NULL,
    available_at_policy_id bigint NOT NULL REFERENCES silver.available_at_policies(id),
    raw_object_id bigint NOT NULL REFERENCES silver.raw_objects(id),
    normalized_by_run_id bigint NOT NULL REFERENCES silver.analytics_runs(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (security_id, date),
    CHECK (open > 0),
    CHECK (high > 0),
    CHECK (low > 0),
    CHECK (close > 0),
    CHECK (adj_close > 0),
    CHECK (volume >= 0),
    CHECK (high >= low),
    CHECK (high >= open AND high >= close),
    CHECK (low <= open AND low <= close),
    CHECK (btrim(currency) <> ''),
    CHECK (btrim(source_system) <> ''),
    CHECK (btrim(normalization_version) <> ''),
    CHECK (available_at >= (date::timestamp AT TIME ZONE 'America/New_York'))
);

CREATE INDEX prices_daily_date_idx
    ON silver.prices_daily (date);

CREATE INDEX prices_daily_raw_object_idx
    ON silver.prices_daily (raw_object_id);

CREATE TABLE silver.feature_definitions (
    id bigserial PRIMARY KEY,
    name text NOT NULL,
    version integer NOT NULL,
    kind text NOT NULL DEFAULT 'numeric',
    computation_spec jsonb NOT NULL,
    definition_hash text NOT NULL,
    notes text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (name, version),
    UNIQUE (definition_hash),
    CHECK (btrim(name) <> ''),
    CHECK (version > 0),
    CHECK (kind = 'numeric'),
    CHECK (jsonb_typeof(computation_spec) = 'object'),
    CHECK (definition_hash ~ '^[0-9a-f]{64}$')
);

CREATE TABLE silver.feature_values (
    id bigserial PRIMARY KEY,
    security_id bigint NOT NULL REFERENCES silver.securities(id),
    asof_date date NOT NULL,
    feature_definition_id bigint NOT NULL
        REFERENCES silver.feature_definitions(id) ON DELETE RESTRICT,
    value double precision NOT NULL,
    available_at timestamptz NOT NULL,
    available_at_policy_id bigint NOT NULL REFERENCES silver.available_at_policies(id),
    computed_by_run_id bigint NOT NULL REFERENCES silver.analytics_runs(id),
    computed_at timestamptz NOT NULL DEFAULT now(),
    source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (security_id, asof_date, feature_definition_id),
    CHECK (computed_at >= available_at),
    CHECK (jsonb_typeof(source_metadata) = 'object')
);

CREATE INDEX feature_values_asof_idx
    ON silver.feature_values (asof_date, feature_definition_id);

CREATE FUNCTION silver.prevent_referenced_feature_definition_changes()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM silver.feature_values
        WHERE feature_definition_id = OLD.id
        LIMIT 1
    ) THEN
        RAISE EXCEPTION
            'feature definition % is immutable once referenced',
            OLD.id;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER feature_definitions_immutable_when_referenced
    BEFORE UPDATE OR DELETE ON silver.feature_definitions
    FOR EACH ROW
    EXECUTE FUNCTION silver.prevent_referenced_feature_definition_changes();

CREATE TABLE silver.forward_return_labels (
    id bigserial PRIMARY KEY,
    security_id bigint NOT NULL REFERENCES silver.securities(id),
    label_date date NOT NULL,
    horizon_days integer NOT NULL,
    horizon_date date NOT NULL,
    horizon_close_at timestamptz NOT NULL,
    label_version integer NOT NULL DEFAULT 1,
    start_adj_close numeric(18, 6) NOT NULL,
    end_adj_close numeric(18, 6) NOT NULL,
    realized_raw_return double precision NOT NULL,
    benchmark_security_id bigint REFERENCES silver.securities(id),
    realized_excess_return double precision,
    available_at timestamptz NOT NULL,
    available_at_policy_id bigint NOT NULL REFERENCES silver.available_at_policies(id),
    computed_by_run_id bigint NOT NULL REFERENCES silver.analytics_runs(id),
    computed_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (security_id, label_date, horizon_days, label_version),
    FOREIGN KEY (security_id, label_date)
        REFERENCES silver.prices_daily (security_id, date),
    FOREIGN KEY (security_id, horizon_date)
        REFERENCES silver.prices_daily (security_id, date),
    CHECK (horizon_days IN (5, 21, 63, 126, 252)),
    CHECK (horizon_date > label_date),
    CHECK (label_version > 0),
    CHECK (start_adj_close > 0),
    CHECK (end_adj_close > 0),
    CHECK (available_at >= horizon_close_at),
    CHECK (computed_at >= available_at),
    CHECK (realized_excess_return IS NULL OR benchmark_security_id IS NOT NULL),
    CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX forward_return_labels_horizon_available_idx
    ON silver.forward_return_labels (horizon_days, available_at);
