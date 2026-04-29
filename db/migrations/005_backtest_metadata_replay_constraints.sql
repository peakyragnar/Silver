ALTER TABLE silver.model_runs
    ADD CONSTRAINT model_runs_cost_assumptions_nonempty
    CHECK (cost_assumptions <> '{}'::jsonb)
    NOT VALID;

ALTER TABLE silver.model_runs
    ADD CONSTRAINT model_runs_policy_versions_nonempty
    CHECK (available_at_policy_versions <> '{}'::jsonb)
    NOT VALID;

ALTER TABLE silver.model_runs
    ADD CONSTRAINT model_runs_replay_inputs_present
    CHECK (
        feature_snapshot_ref IS NOT NULL
        OR input_fingerprints <> '{}'::jsonb
    )
    NOT VALID;

ALTER TABLE silver.backtest_runs
    ADD CONSTRAINT backtest_runs_cost_assumptions_nonempty
    CHECK (cost_assumptions <> '{}'::jsonb)
    NOT VALID;

ALTER TABLE silver.backtest_runs
    ADD CONSTRAINT backtest_runs_succeeded_claim_payloads_nonempty
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
    NOT VALID;
