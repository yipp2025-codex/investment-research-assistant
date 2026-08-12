CREATE INDEX IF NOT EXISTS idx_pipeline_runs_provider_market_date
    ON pipeline_runs(provider, market_date);
