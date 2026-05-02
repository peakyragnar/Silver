ALTER TABLE silver.earnings_release_events
    ADD COLUMN release_timing text;

UPDATE silver.earnings_release_events AS event
SET release_timing =
    CASE
        WHEN NOT COALESCE(
            (
                SELECT calendar.is_session
                FROM silver.trading_calendar AS calendar
                WHERE calendar.date = (
                    event.accepted_at AT TIME ZONE 'America/New_York'
                )::date
            ),
            false
        ) THEN 'non_trading_day'
        WHEN (
            event.accepted_at AT TIME ZONE 'America/New_York'
        )::time < time '09:30' THEN 'bmo'
        WHEN (
            event.accepted_at AT TIME ZONE 'America/New_York'
        )::time >= time '16:00' THEN 'amc'
        ELSE 'rth'
    END;

ALTER TABLE silver.earnings_release_events
    ALTER COLUMN release_timing SET NOT NULL;

ALTER TABLE silver.earnings_release_events
    ADD CONSTRAINT earnings_release_events_release_timing_check
    CHECK (release_timing IN ('bmo', 'rth', 'amc', 'non_trading_day'));

DROP VIEW silver.earnings_release_fundamental_values;

CREATE VIEW silver.earnings_release_fundamental_values AS
SELECT
    event.id AS earnings_release_event_id,
    value.id AS fundamental_value_id,
    event.security_id,
    event.fiscal_year,
    event.fiscal_period,
    event.period_end_date,
    event.release_timing,
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
