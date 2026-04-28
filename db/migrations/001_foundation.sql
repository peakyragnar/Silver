CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE silver.securities (
    id bigserial PRIMARY KEY,
    ticker text NOT NULL UNIQUE,
    name text NOT NULL,
    cik text,
    exchange text,
    asset_class text NOT NULL DEFAULT 'equity',
    country text NOT NULL DEFAULT 'US',
    currency text NOT NULL DEFAULT 'USD',
    fiscal_year_end_md text,
    listed_at date,
    delisted_at date,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (delisted_at IS NULL OR listed_at IS NULL OR delisted_at >= listed_at)
);

CREATE TABLE silver.security_identifiers (
    security_id bigint NOT NULL REFERENCES silver.securities(id),
    identifier_type text NOT NULL,
    identifier text NOT NULL,
    valid_from date NOT NULL,
    valid_to date,
    PRIMARY KEY (security_id, identifier_type, valid_from),
    CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE INDEX security_identifiers_lookup_idx
    ON silver.security_identifiers (identifier_type, identifier);

CREATE TABLE silver.trading_calendar (
    date date PRIMARY KEY,
    is_session boolean NOT NULL,
    session_close timestamptz,
    is_early_close boolean NOT NULL DEFAULT false,
    CHECK (is_session OR session_close IS NULL),
    CHECK (is_session OR NOT is_early_close)
);

CREATE TABLE silver.universe_membership (
    security_id bigint NOT NULL REFERENCES silver.securities(id),
    universe_name text NOT NULL,
    valid_from date NOT NULL,
    valid_to date,
    reason text,
    PRIMARY KEY (security_id, universe_name, valid_from),
    CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE INDEX universe_membership_lookup_idx
    ON silver.universe_membership (universe_name, valid_from, valid_to);

CREATE TABLE silver.raw_objects (
    id bigserial PRIMARY KEY,
    vendor text NOT NULL,
    endpoint text NOT NULL,
    params_hash text NOT NULL,
    params jsonb NOT NULL,
    request_url text NOT NULL,
    http_status integer NOT NULL,
    content_type text,
    body_jsonb jsonb,
    body_raw bytea,
    raw_hash text NOT NULL,
    fetched_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (vendor, endpoint, params_hash, raw_hash),
    CHECK (http_status BETWEEN 100 AND 599),
    CHECK (body_jsonb IS NOT NULL OR body_raw IS NOT NULL)
);

CREATE INDEX raw_objects_vendor_endpoint_fetched_at_idx
    ON silver.raw_objects (vendor, endpoint, fetched_at);

CREATE TABLE silver.available_at_policies (
    id bigserial PRIMARY KEY,
    name text NOT NULL,
    version integer NOT NULL,
    rule jsonb NOT NULL,
    valid_from timestamptz NOT NULL DEFAULT now(),
    valid_to timestamptz,
    notes text,
    UNIQUE (name, version),
    CHECK (version > 0),
    CHECK (valid_to IS NULL OR valid_to > valid_from)
);

CREATE INDEX available_at_policies_active_idx
    ON silver.available_at_policies (name, valid_from, valid_to);
