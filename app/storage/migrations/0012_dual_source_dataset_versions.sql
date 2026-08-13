PRAGMA foreign_keys = ON;

CREATE TABLE research_dataset_versions (
    dataset_version_id TEXT PRIMARY KEY
        CHECK (length(dataset_version_id) = 64),
    symbol TEXT NOT NULL,
    as_of_date TEXT NOT NULL
        CHECK (as_of_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    contract_version TEXT NOT NULL
        CHECK (contract_version = 'dual-source-dataset-contract.v1'),
    persistence_contract_version TEXT NOT NULL
        CHECK (persistence_contract_version = 'mixed-dataset-persistence-contract.v1'),
    methodology_version TEXT NOT NULL
        CHECK (length(trim(methodology_version)) > 0),
    source_policy TEXT NOT NULL
        CHECK (source_policy IN ('twse_baseline', 'twse_dual_source_v1')),
    source_status TEXT NOT NULL
        CHECK (source_status IN ('canonical_complete', 'provisional_mixed', 'reconciled')),
    authority_status TEXT NOT NULL
        CHECK (authority_status IN ('complete', 'incomplete', 'reconciled')),
    reconciliation_status TEXT NOT NULL
        CHECK (reconciliation_status IN (
            'not_applicable', 'pending', 'reconciled_equal', 'reconciled_discrepant'
        )),
    coverage_basis TEXT NOT NULL
        CHECK (coverage_basis IN ('standard', 'legal_short_listing_history')),
    required_observation_count INTEGER NOT NULL
        CHECK (required_observation_count > 0),
    twse_observation_count INTEGER NOT NULL
        CHECK (twse_observation_count >= 0),
    esun_supplemental_count INTEGER NOT NULL
        CHECK (esun_supplemental_count >= 0),
    missing_twse_count INTEGER NOT NULL
        CHECK (missing_twse_count >= 0),
    selected_observation_count INTEGER NOT NULL
        CHECK (selected_observation_count >= 0),
    discrepancy_count INTEGER NOT NULL
        CHECK (discrepancy_count >= 0),
    coverage_complete INTEGER NOT NULL
        CHECK (coverage_complete IN (0, 1)),
    latest_reconciled_date TEXT
        CHECK (
            latest_reconciled_date IS NULL OR
            latest_reconciled_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
        ),
    provenance_map_sha256 TEXT NOT NULL
        CHECK (length(provenance_map_sha256) = 64),
    parent_dataset_version_id TEXT,
    canonical_sha256 TEXT NOT NULL
        CHECK (length(canonical_sha256) = 64),
    created_at TEXT NOT NULL
        CHECK (length(trim(created_at)) > 0),
    FOREIGN KEY (symbol) REFERENCES symbols(symbol) ON UPDATE CASCADE,
    FOREIGN KEY (parent_dataset_version_id)
        REFERENCES research_dataset_versions(dataset_version_id),
    CHECK (twse_observation_count + esun_supplemental_count <= required_observation_count),
    CHECK (missing_twse_count = required_observation_count - twse_observation_count),
    CHECK (
        selected_observation_count = twse_observation_count + esun_supplemental_count
    ),
    CHECK (
        coverage_complete = CASE
            WHEN selected_observation_count = required_observation_count THEN 1
            ELSE 0
        END
    ),
    CHECK (
        (source_status = 'canonical_complete'
            AND authority_status = 'complete'
            AND reconciliation_status = 'not_applicable'
            AND esun_supplemental_count = 0
            AND twse_observation_count = required_observation_count
            AND parent_dataset_version_id IS NULL)
        OR
        (source_status = 'provisional_mixed'
            AND authority_status = 'incomplete'
            AND reconciliation_status = 'pending'
            AND esun_supplemental_count > 0
            AND source_policy = 'twse_dual_source_v1')
        OR
        (source_status = 'reconciled'
            AND authority_status = 'reconciled'
            AND reconciliation_status IN ('reconciled_equal', 'reconciled_discrepant')
            AND esun_supplemental_count = 0
            AND twse_observation_count = required_observation_count
            AND source_policy = 'twse_dual_source_v1'
            AND parent_dataset_version_id IS NOT NULL)
    ),
    CHECK (parent_dataset_version_id IS NULL OR parent_dataset_version_id <> dataset_version_id)
);

CREATE TABLE research_dataset_observations (
    dataset_version_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trade_date TEXT NOT NULL
        CHECK (trade_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    provider TEXT NOT NULL
        CHECK (provider IN ('twse', 'twse-historical', 'esun', 'esun-historical')),
    source_role TEXT NOT NULL
        CHECK (source_role IN ('canonical', 'supplemental', 'validation')),
    source_run_id TEXT NOT NULL
        CHECK (length(trim(source_run_id)) > 0),
    selected INTEGER NOT NULL CHECK (selected IN (0, 1)),
    open_price REAL NOT NULL CHECK (open_price > 0),
    high_price REAL NOT NULL CHECK (high_price > 0),
    low_price REAL NOT NULL CHECK (low_price > 0),
    close_price REAL NOT NULL CHECK (close_price > 0),
    volume INTEGER NOT NULL CHECK (volume >= 0),
    observation_sha256 TEXT NOT NULL
        CHECK (length(observation_sha256) = 64),
    PRIMARY KEY (dataset_version_id, trade_date, provider, source_role),
    FOREIGN KEY (dataset_version_id)
        REFERENCES research_dataset_versions(dataset_version_id) ON DELETE CASCADE,
    FOREIGN KEY (symbol) REFERENCES symbols(symbol) ON UPDATE CASCADE,
    CHECK (high_price >= open_price),
    CHECK (high_price >= low_price),
    CHECK (high_price >= close_price),
    CHECK (low_price <= open_price),
    CHECK (low_price <= high_price),
    CHECK (low_price <= close_price),
    CHECK (
        (source_role = 'canonical'
            AND provider IN ('twse', 'twse-historical')
            AND selected = 1)
        OR
        (source_role = 'supplemental'
            AND provider IN ('esun', 'esun-historical')
            AND selected = 1)
        OR
        (source_role = 'validation'
            AND selected = 0)
    )
);

CREATE TABLE research_dataset_artifacts (
    dataset_version_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    provider TEXT NOT NULL
        CHECK (provider IN ('twse', 'twse-historical', 'esun', 'esun-historical')),
    dataset TEXT NOT NULL CHECK (length(trim(dataset)) > 0),
    source_ref TEXT NOT NULL CHECK (length(trim(source_ref)) > 0),
    contract_version TEXT NOT NULL CHECK (length(trim(contract_version)) > 0),
    payload_sha256 TEXT NOT NULL CHECK (length(payload_sha256) = 64),
    payload_size_bytes INTEGER NOT NULL CHECK (payload_size_bytes >= 0),
    hash_basis TEXT NOT NULL
        CHECK (hash_basis IN ('raw-response-bytes-v1', 'canonical-json-v1')),
    PRIMARY KEY (dataset_version_id, ordinal),
    FOREIGN KEY (dataset_version_id)
        REFERENCES research_dataset_versions(dataset_version_id) ON DELETE CASCADE,
    CHECK (lower(source_ref) NOT LIKE '%authorization=%'),
    CHECK (lower(source_ref) NOT LIKE '%api_key=%'),
    CHECK (lower(source_ref) NOT LIKE '%apikey=%'),
    CHECK (lower(source_ref) NOT LIKE '%password=%'),
    CHECK (lower(source_ref) NOT LIKE '%secret=%'),
    CHECK (lower(source_ref) NOT LIKE '%token=%')
);

CREATE INDEX idx_research_dataset_versions_symbol_date
    ON research_dataset_versions(symbol, as_of_date);
CREATE INDEX idx_research_dataset_versions_parent
    ON research_dataset_versions(parent_dataset_version_id);
CREATE INDEX idx_research_dataset_observations_date
    ON research_dataset_observations(dataset_version_id, trade_date);
CREATE INDEX idx_research_dataset_observations_provider_role
    ON research_dataset_observations(dataset_version_id, provider, source_role);
CREATE UNIQUE INDEX ux_research_dataset_observations_selected_date
    ON research_dataset_observations(dataset_version_id, trade_date)
    WHERE selected = 1;
CREATE INDEX idx_research_dataset_artifacts_payload
    ON research_dataset_artifacts(payload_sha256);
CREATE INDEX idx_research_dataset_artifacts_provider_dataset
    ON research_dataset_artifacts(provider, dataset);

-- DS5 consumer metadata is additive.  The frozen v11 ``status`` column and
-- its terminal trigger remain untouched so legacy v1 rows and replay hashes
-- cannot be rewritten.  ``ds5_execution_status`` is the explicit DS5
-- terminal status; ``provisional_success`` is never mapped to legacy
-- ``partial_success``.
ALTER TABLE screener_runs ADD COLUMN ds5_execution_status TEXT NOT NULL DEFAULT 'success'
    CHECK (ds5_execution_status IN ('success', 'provisional_success'));
ALTER TABLE screener_runs ADD COLUMN ds5_source_policy TEXT NOT NULL DEFAULT 'twse_baseline'
    CHECK (ds5_source_policy IN ('twse_baseline', 'twse_dual_source_v1'));
ALTER TABLE screener_runs ADD COLUMN ds5_source_status TEXT NOT NULL DEFAULT 'canonical_complete'
    CHECK (ds5_source_status IN ('canonical_complete', 'provisional_mixed', 'reconciled'));
ALTER TABLE screener_runs ADD COLUMN ds5_research_data_quality TEXT NOT NULL DEFAULT 'canonical'
    CHECK (ds5_research_data_quality IN ('canonical', 'provisional', 'reconciled'));
ALTER TABLE screener_runs ADD COLUMN ds5_authority_status TEXT NOT NULL DEFAULT 'complete'
    CHECK (ds5_authority_status IN ('complete', 'incomplete', 'reconciled'));
ALTER TABLE screener_runs ADD COLUMN ds5_reconciliation_status TEXT NOT NULL DEFAULT 'not_applicable'
    CHECK (ds5_reconciliation_status IN (
        'not_applicable', 'pending', 'reconciled_equal', 'reconciled_discrepant'
    ));
ALTER TABLE screener_runs ADD COLUMN ds5_supplemental_candidate_count INTEGER NOT NULL DEFAULT 0
    CHECK (ds5_supplemental_candidate_count >= 0);
ALTER TABLE screener_runs ADD COLUMN ds5_canonical_authority TEXT NOT NULL DEFAULT 'twse'
    CHECK (ds5_canonical_authority = 'twse');
ALTER TABLE screener_runs ADD COLUMN ds5_supplemental_sources_json TEXT NOT NULL DEFAULT '[]'
    CHECK (json_valid(ds5_supplemental_sources_json) AND json_type(ds5_supplemental_sources_json) = 'array');
ALTER TABLE screener_runs ADD COLUMN ds5_twse_observation_count INTEGER NOT NULL DEFAULT 0
    CHECK (ds5_twse_observation_count >= 0);
ALTER TABLE screener_runs ADD COLUMN ds5_missing_twse_count INTEGER NOT NULL DEFAULT 0
    CHECK (ds5_missing_twse_count >= 0);
ALTER TABLE screener_runs ADD COLUMN ds5_discrepancy_count INTEGER NOT NULL DEFAULT 0
    CHECK (ds5_discrepancy_count >= 0);
ALTER TABLE screener_runs ADD COLUMN ds5_dataset_identity_sha256 TEXT
    CHECK (ds5_dataset_identity_sha256 IS NULL OR length(ds5_dataset_identity_sha256) = 64);
ALTER TABLE screener_runs ADD COLUMN ds5_provenance_map_sha256 TEXT
    CHECK (ds5_provenance_map_sha256 IS NULL OR length(ds5_provenance_map_sha256) = 64);
ALTER TABLE screener_runs ADD COLUMN ds5_dataset_version_ids_json TEXT NOT NULL DEFAULT '[]'
    CHECK (json_valid(ds5_dataset_version_ids_json) AND json_type(ds5_dataset_version_ids_json) = 'array');

ALTER TABLE screener_candidates ADD COLUMN ds5_dataset_version_id TEXT
    CHECK (ds5_dataset_version_id IS NULL OR length(ds5_dataset_version_id) = 64);
ALTER TABLE screener_candidates ADD COLUMN ds5_source_policy TEXT NOT NULL DEFAULT 'twse_baseline'
    CHECK (ds5_source_policy IN ('twse_baseline', 'twse_dual_source_v1'));
ALTER TABLE screener_candidates ADD COLUMN ds5_source_status TEXT NOT NULL DEFAULT 'canonical_complete'
    CHECK (ds5_source_status IN ('canonical_complete', 'provisional_mixed', 'reconciled'));
ALTER TABLE screener_candidates ADD COLUMN ds5_authority_status TEXT NOT NULL DEFAULT 'complete'
    CHECK (ds5_authority_status IN ('complete', 'incomplete', 'reconciled'));
ALTER TABLE screener_candidates ADD COLUMN ds5_reconciliation_status TEXT NOT NULL DEFAULT 'not_applicable'
    CHECK (ds5_reconciliation_status IN (
        'not_applicable', 'pending', 'reconciled_equal', 'reconciled_discrepant'
    ));
ALTER TABLE screener_candidates ADD COLUMN ds5_research_data_quality TEXT NOT NULL DEFAULT 'canonical'
    CHECK (ds5_research_data_quality IN ('canonical', 'provisional', 'reconciled'));
ALTER TABLE screener_candidates ADD COLUMN ds5_canonical_authority TEXT NOT NULL DEFAULT 'twse'
    CHECK (ds5_canonical_authority = 'twse');
ALTER TABLE screener_candidates ADD COLUMN ds5_supplemental_sources_json TEXT NOT NULL DEFAULT '[]'
    CHECK (json_valid(ds5_supplemental_sources_json) AND json_type(ds5_supplemental_sources_json) = 'array');
ALTER TABLE screener_candidates ADD COLUMN ds5_twse_observation_count INTEGER NOT NULL DEFAULT 0
    CHECK (ds5_twse_observation_count >= 0);
ALTER TABLE screener_candidates ADD COLUMN ds5_esun_supplemental_count INTEGER NOT NULL DEFAULT 0
    CHECK (ds5_esun_supplemental_count >= 0);
ALTER TABLE screener_candidates ADD COLUMN ds5_missing_twse_count INTEGER NOT NULL DEFAULT 0
    CHECK (ds5_missing_twse_count >= 0);
ALTER TABLE screener_candidates ADD COLUMN ds5_discrepancy_count INTEGER NOT NULL DEFAULT 0
    CHECK (ds5_discrepancy_count >= 0);
ALTER TABLE screener_candidates ADD COLUMN ds5_provenance_map_sha256 TEXT
    CHECK (ds5_provenance_map_sha256 IS NULL OR length(ds5_provenance_map_sha256) = 64);
ALTER TABLE screener_candidates ADD COLUMN ds5_parent_dataset_version_id TEXT
    CHECK (ds5_parent_dataset_version_id IS NULL OR length(ds5_parent_dataset_version_id) = 64);

CREATE INDEX idx_screener_runs_ds5_quality
    ON screener_runs(ds5_research_data_quality, ds5_execution_status);
CREATE INDEX idx_screener_candidates_ds5_dataset
    ON screener_candidates(ds5_dataset_version_id);
