ALTER TABLE silver.analytics_runs
    DROP CONSTRAINT analytics_runs_run_kind_check;

ALTER TABLE silver.analytics_runs
    ADD CONSTRAINT analytics_runs_run_kind_check
    CHECK (
        run_kind IN (
            'price_normalization',
            'label_generation',
            'feature_generation',
            'backtest',
            'falsifier_report_invocation',
            'sec_companyfacts_ingest',
            'fmp_fundamentals_normalization',
            'sec_earnings_release_ingest'
        )
    );

CREATE TABLE silver.earnings_release_events (
    id bigserial PRIMARY KEY,
    security_id bigint NOT NULL REFERENCES silver.securities(id),
    event_type text NOT NULL DEFAULT 'earnings_release',
    source_system text NOT NULL DEFAULT 'sec',
    accession_number text NOT NULL,
    form_type text NOT NULL,
    item_codes text NOT NULL,
    filing_date date NOT NULL,
    report_date date,
    accepted_at timestamptz NOT NULL,
    release_available_at timestamptz NOT NULL,
    available_at_policy_id bigint NOT NULL REFERENCES silver.available_at_policies(id),
    fiscal_year integer NOT NULL,
    fiscal_period text NOT NULL,
    period_end_date date NOT NULL,
    primary_document text NOT NULL,
    exhibit_document text,
    submissions_raw_object_id bigint NOT NULL REFERENCES silver.raw_objects(id),
    archive_index_raw_object_id bigint REFERENCES silver.raw_objects(id),
    exhibit_raw_object_id bigint REFERENCES silver.raw_objects(id),
    normalized_by_run_id bigint NOT NULL REFERENCES silver.analytics_runs(id),
    matched_confidence text NOT NULL,
    match_method text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (security_id, accession_number),
    CHECK (event_type = 'earnings_release'),
    CHECK (source_system IN ('sec', 'fmp')),
    CHECK (accession_number ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    CHECK (form_type IN ('8-K', '8-K/A')),
    CHECK (item_codes LIKE '%2.02%'),
    CHECK (release_available_at > accepted_at),
    CHECK (fiscal_year BETWEEN 1900 AND 2100),
    CHECK (fiscal_period IN ('FY', 'Q1', 'Q2', 'Q3', 'Q4')),
    CHECK (btrim(primary_document) <> ''),
    CHECK (matched_confidence IN ('high', 'medium', 'low')),
    CHECK (btrim(match_method) <> ''),
    CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX earnings_release_events_security_period_idx
    ON silver.earnings_release_events (
        security_id,
        fiscal_year,
        fiscal_period,
        period_end_date
    );

CREATE INDEX earnings_release_events_available_at_idx
    ON silver.earnings_release_events (release_available_at);

CREATE VIEW silver.earnings_release_fundamental_values AS
SELECT
    event.id AS earnings_release_event_id,
    value.id AS fundamental_value_id,
    event.security_id,
    event.fiscal_year,
    event.fiscal_period,
    event.period_end_date,
    event.release_available_at,
    value.available_at AS filing_available_at,
    value.statement_type,
    value.metric_name,
    value.metric_value,
    value.currency
FROM silver.earnings_release_events AS event
JOIN silver.fundamental_values AS value
  ON value.security_id = event.security_id
 AND value.fiscal_year = event.fiscal_year
 AND value.fiscal_period = event.fiscal_period
 AND value.period_end_date = event.period_end_date
WHERE value.statement_type = 'income_statement';
