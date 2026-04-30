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
            'falsifier_report_invocation'
        )
    );
