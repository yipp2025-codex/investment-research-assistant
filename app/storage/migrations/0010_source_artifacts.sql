CREATE TABLE IF NOT EXISTS source_artifacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_run_id TEXT,
    historical_run_id TEXT,
    validation_run_id TEXT,
    checkpoint_key TEXT NOT NULL CHECK (length(trim(checkpoint_key)) > 0),
    provider TEXT NOT NULL CHECK (length(trim(provider)) > 0),
    dataset TEXT NOT NULL CHECK (length(trim(dataset)) > 0),
    endpoint TEXT NOT NULL CHECK (length(trim(endpoint)) > 0),
    contract_version TEXT NOT NULL CHECK (length(trim(contract_version)) > 0),
    content_type TEXT NOT NULL CHECK (length(trim(content_type)) > 0),
    payload_sha256 TEXT NOT NULL CHECK (
        length(payload_sha256) = 64
        AND payload_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    payload_size_bytes INTEGER NOT NULL CHECK (payload_size_bytes >= 0),
    hash_basis TEXT NOT NULL CHECK (
        hash_basis IN ('raw-response-bytes-v1', 'canonical-json-v1')
    ),
    fetched_at TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(run_id)
        ON DELETE CASCADE,
    FOREIGN KEY (historical_run_id) REFERENCES historical_sync_runs(run_id)
        ON DELETE CASCADE,
    FOREIGN KEY (validation_run_id) REFERENCES market_data_validation_runs(run_id)
        ON DELETE CASCADE,
    CHECK (
        (pipeline_run_id IS NOT NULL)
        + (historical_run_id IS NOT NULL)
        + (validation_run_id IS NOT NULL) = 1
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_source_artifacts_pipeline_unique
    ON source_artifacts(
        pipeline_run_id, checkpoint_key, provider, dataset, endpoint, payload_sha256
    ) WHERE pipeline_run_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_source_artifacts_historical_unique
    ON source_artifacts(
        historical_run_id, checkpoint_key, provider, dataset, endpoint, payload_sha256
    ) WHERE historical_run_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_source_artifacts_validation_unique
    ON source_artifacts(
        validation_run_id, checkpoint_key, provider, dataset, endpoint, payload_sha256
    ) WHERE validation_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_source_artifacts_payload_sha256
    ON source_artifacts(payload_sha256);
CREATE INDEX IF NOT EXISTS idx_source_artifacts_provider_dataset
    ON source_artifacts(provider, dataset, created_at);
