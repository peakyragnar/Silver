ALTER TABLE silver.raw_objects
    ADD COLUMN metadata jsonb NOT NULL DEFAULT '{}'::jsonb;
