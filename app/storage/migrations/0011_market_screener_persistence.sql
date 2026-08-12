PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS market_universe_runs (
    universe_run_id TEXT PRIMARY KEY CHECK (
        length(universe_run_id) = 64
        AND universe_run_id NOT GLOB '*[^0-9a-f]*'
    ),
    market_date TEXT NOT NULL CHECK (
        length(market_date) = 10
        AND date(market_date) IS NOT NULL
        AND date(market_date) = market_date
    ),
    methodology_version TEXT NOT NULL CHECK (length(trim(methodology_version)) > 0),
    source_policy TEXT NOT NULL CHECK (source_policy = 'twse_baseline'),
    input_evidence_sha256 TEXT NOT NULL CHECK (
        length(input_evidence_sha256) = 64
        AND input_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    universe_count INTEGER NOT NULL CHECK (universe_count >= 0),
    scan_eligible_count INTEGER NOT NULL CHECK (scan_eligible_count >= 0),
    scan_unavailable_count INTEGER NOT NULL CHECK (scan_unavailable_count >= 0),
    excluded_count INTEGER NOT NULL CHECK (excluded_count >= 0),
    inactive_count INTEGER NOT NULL CHECK (inactive_count >= 0),
    unresolved_count INTEGER NOT NULL CHECK (unresolved_count >= 0),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'success', 'failed')),
    canonical_sha256 TEXT CHECK (
        canonical_sha256 IS NULL
        OR (
            length(canonical_sha256) = 64
            AND canonical_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    error_code TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (
        market_date, methodology_version, source_policy, input_evidence_sha256
    ),
    UNIQUE (universe_run_id, market_date),
    CHECK (
        universe_count = scan_eligible_count + scan_unavailable_count
            + excluded_count + inactive_count + unresolved_count
    ),
    CHECK (
        (status = 'success' AND canonical_sha256 IS NOT NULL AND finished_at IS NOT NULL)
        OR (status <> 'success' AND canonical_sha256 IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS market_universe_members (
    universe_run_id TEXT NOT NULL,
    symbol TEXT NOT NULL CHECK (
        length(symbol) BETWEEN 2 AND 12
        AND symbol NOT GLOB '*[^0-9A-Z]*'
    ),
    name TEXT CHECK (name IS NULL OR length(trim(name)) > 0),
    market TEXT NOT NULL CHECK (market = 'TWSE'),
    status TEXT NOT NULL CHECK (status IN (
        'active_scan_eligible',
        'active_scan_unavailable',
        'excluded_non_common_equity',
        'inactive',
        'classification_unresolved'
    )),
    listing_date TEXT CHECK (
        listing_date IS NULL
        OR (
            length(listing_date) = 10
            AND date(listing_date) IS NOT NULL
            AND date(listing_date) = listing_date
        )
    ),
    delisting_date TEXT CHECK (
        delisting_date IS NULL
        OR (
            length(delisting_date) = 10
            AND date(delisting_date) IS NOT NULL
            AND date(delisting_date) = delisting_date
        )
    ),
    exclusion_reason TEXT,
    PRIMARY KEY (universe_run_id, symbol),
    FOREIGN KEY (universe_run_id) REFERENCES market_universe_runs(universe_run_id)
        ON DELETE RESTRICT,
    CHECK (
        (status = 'active_scan_eligible' AND exclusion_reason IS NULL)
        OR (
            status <> 'active_scan_eligible'
            AND exclusion_reason IS NOT NULL
            AND length(trim(exclusion_reason)) > 0
        )
    ),
    CHECK (
        listing_date IS NULL OR delisting_date IS NULL OR delisting_date >= listing_date
    )
);

CREATE TABLE IF NOT EXISTS screener_runs (
    screener_run_id TEXT PRIMARY KEY CHECK (
        length(screener_run_id) = 64
        AND screener_run_id NOT GLOB '*[^0-9a-f]*'
    ),
    market_date TEXT NOT NULL CHECK (
        length(market_date) = 10
        AND date(market_date) IS NOT NULL
        AND date(market_date) = market_date
    ),
    universe_run_id TEXT NOT NULL,
    stage1_methodology_version TEXT NOT NULL
        CHECK (length(trim(stage1_methodology_version)) > 0),
    stage2_methodology_version TEXT NOT NULL
        CHECK (length(trim(stage2_methodology_version)) > 0),
    source_policy TEXT NOT NULL CHECK (source_policy = 'twse_baseline'),
    candidate_limit INTEGER NOT NULL CHECK (candidate_limit > 0),
    input_manifest_sha256 TEXT NOT NULL CHECK (
        length(input_manifest_sha256) = 64
        AND input_manifest_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    stage1_canonical_sha256 TEXT NOT NULL CHECK (
        length(stage1_canonical_sha256) = 64
        AND stage1_canonical_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    universe_count INTEGER NOT NULL CHECK (universe_count >= 0),
    screened_count INTEGER NOT NULL CHECK (screened_count >= 0),
    triggered_count INTEGER NOT NULL CHECK (triggered_count >= 0),
    candidate_count INTEGER NOT NULL CHECK (candidate_count >= 0),
    truncated INTEGER NOT NULL CHECK (truncated IN (0, 1)),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (
        status IN ('pending', 'running', 'partial_success', 'success', 'failed')
    ),
    canonical_sha256 TEXT CHECK (
        canonical_sha256 IS NULL
        OR (
            length(canonical_sha256) = 64
            AND canonical_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    error_code TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (
        market_date,
        universe_run_id,
        stage1_methodology_version,
        stage2_methodology_version,
        source_policy,
        candidate_limit,
        input_manifest_sha256
    ),
    UNIQUE (screener_run_id, universe_run_id),
    FOREIGN KEY (universe_run_id, market_date)
        REFERENCES market_universe_runs(universe_run_id, market_date)
        ON DELETE RESTRICT,
    CHECK (screened_count <= universe_count),
    CHECK (triggered_count <= screened_count),
    CHECK (candidate_count <= triggered_count),
    CHECK (candidate_count <= candidate_limit),
    CHECK (
        (truncated = 1 AND triggered_count > candidate_count)
        OR (truncated = 0 AND triggered_count = candidate_count)
    ),
    CHECK (
        (status = 'success' AND canonical_sha256 IS NOT NULL AND finished_at IS NOT NULL)
        OR (status <> 'success' AND canonical_sha256 IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS screener_candidates (
    candidate_id TEXT PRIMARY KEY CHECK (
        length(candidate_id) = 64
        AND candidate_id NOT GLOB '*[^0-9a-f]*'
    ),
    screener_run_id TEXT NOT NULL,
    universe_run_id TEXT NOT NULL,
    symbol TEXT NOT NULL CHECK (
        length(symbol) BETWEEN 2 AND 12
        AND symbol NOT GLOB '*[^0-9A-Z]*'
    ),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'success', 'failed')),
    rank INTEGER CHECK (rank IS NULL OR rank > 0),
    stage1_rank INTEGER NOT NULL CHECK (stage1_rank > 0),
    candidate_kind TEXT CHECK (
        candidate_kind IS NULL
        OR candidate_kind IN ('research_candidate', 'data_quality_candidate')
    ),
    stage1_trigger_count INTEGER NOT NULL CHECK (stage1_trigger_count > 0),
    stage1_reason_count INTEGER NOT NULL CHECK (
        stage1_reason_count >= stage1_trigger_count
    ),
    stage2_reason_count INTEGER NOT NULL DEFAULT 0 CHECK (stage2_reason_count >= 0),
    metric_count INTEGER NOT NULL DEFAULT 0 CHECK (metric_count >= 0),
    data_quality_status TEXT CHECK (
        data_quality_status IS NULL
        OR data_quality_status IN ('clean', 'warning', 'blocked', 'failed')
    ),
    validation_status TEXT CHECK (
        validation_status IS NULL
        OR validation_status IN (
            'available', 'missing_source', 'market_date_mismatch', 'source_discrepancy'
        )
    ),
    analysis_status TEXT CHECK (
        analysis_status IS NULL OR analysis_status IN ('available', 'unavailable', 'failed')
    ),
    pipeline_run_id TEXT,
    historical_run_id TEXT,
    validation_run_id TEXT,
    canonical_sources_json TEXT NOT NULL DEFAULT '[]' CHECK (
        json_valid(canonical_sources_json)
        AND json_type(canonical_sources_json) = 'array'
        AND instr(lower(canonical_sources_json), '"esun"') = 0
        AND instr(lower(canonical_sources_json), '"esun-historical"') = 0
    ),
    validation_sources_json TEXT NOT NULL DEFAULT '[]' CHECK (
        json_valid(validation_sources_json)
        AND json_type(validation_sources_json) = 'array'
    ),
    discrepancies_json TEXT NOT NULL DEFAULT '[]' CHECK (
        json_valid(discrepancies_json)
        AND json_type(discrepancies_json) = 'array'
    ),
    input_locator_sha256 TEXT NOT NULL CHECK (
        length(input_locator_sha256) = 64
        AND input_locator_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    snapshot_sha256 TEXT CHECK (
        snapshot_sha256 IS NULL
        OR (
            length(snapshot_sha256) = 64
            AND snapshot_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    payload_sha256 TEXT CHECK (
        payload_sha256 IS NULL
        OR (
            length(payload_sha256) = 64
            AND payload_sha256 NOT GLOB '*[^0-9a-f]*'
        )
    ),
    failure_code TEXT,
    failure_type TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (screener_run_id, symbol),
    UNIQUE (screener_run_id, stage1_rank),
    FOREIGN KEY (screener_run_id, universe_run_id)
        REFERENCES screener_runs(screener_run_id, universe_run_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (universe_run_id, symbol)
        REFERENCES market_universe_members(universe_run_id, symbol)
        ON DELETE RESTRICT,
    FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(run_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (historical_run_id) REFERENCES historical_sync_runs(run_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (validation_run_id) REFERENCES market_data_validation_runs(run_id)
        ON DELETE RESTRICT,
    CHECK (
        status IN ('pending', 'running')
        OR (
            candidate_kind IS NOT NULL
            AND data_quality_status IS NOT NULL
            AND validation_status IS NOT NULL
            AND analysis_status IS NOT NULL
            AND payload_sha256 IS NOT NULL
        )
    ),
    CHECK (status <> 'success' OR snapshot_sha256 IS NOT NULL),
    CHECK (
        (status = 'failed' AND failure_code IS NOT NULL AND failure_type IS NOT NULL)
        OR (status <> 'failed' AND failure_code IS NULL AND failure_type IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS candidate_reasons (
    candidate_id TEXT NOT NULL,
    stage TEXT NOT NULL CHECK (stage IN ('stage1', 'stage2')),
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    code TEXT NOT NULL CHECK (length(trim(code)) > 0),
    metric TEXT NOT NULL CHECK (length(trim(metric)) > 0),
    component TEXT CHECK (component IS NULL OR length(trim(component)) > 0),
    previous_json TEXT NOT NULL CHECK (
        json_valid(previous_json)
        AND json_type(previous_json) IN ('null', 'integer', 'real', 'text')
    ),
    current_json TEXT NOT NULL CHECK (
        json_valid(current_json)
        AND json_type(current_json) IN ('null', 'integer', 'real', 'text')
    ),
    delta REAL,
    unit TEXT NOT NULL CHECK (length(trim(unit)) > 0),
    operator TEXT NOT NULL CHECK (length(trim(operator)) > 0),
    threshold_json TEXT NOT NULL CHECK (
        json_valid(threshold_json)
        AND json_type(threshold_json) IN ('integer', 'real', 'text')
    ),
    rule_version TEXT NOT NULL CHECK (length(trim(rule_version)) > 0),
    role TEXT CHECK (role IS NULL OR role IN ('primary', 'secondary')),
    trigger_class TEXT CHECK (
        trigger_class IS NULL OR length(trim(trigger_class)) > 0
    ),
    reason_kind TEXT CHECK (
        reason_kind IS NULL OR reason_kind IN ('research_change', 'data_quality')
    ),
    reason_class TEXT CHECK (
        reason_class IS NULL OR length(trim(reason_class)) > 0
    ),
    threshold_multiple REAL NOT NULL CHECK (threshold_multiple >= 0),
    PRIMARY KEY (candidate_id, stage, ordinal),
    FOREIGN KEY (candidate_id) REFERENCES screener_candidates(candidate_id)
        ON DELETE RESTRICT,
    CHECK (
        (
            stage = 'stage1'
            AND role IS NOT NULL
            AND trigger_class IS NOT NULL
            AND reason_kind IS NULL
            AND reason_class IS NULL
        )
        OR (
            stage = 'stage2'
            AND role IS NULL
            AND trigger_class IS NULL
            AND reason_kind IS NOT NULL
            AND reason_class IS NOT NULL
        )
    )
);

CREATE TABLE IF NOT EXISTS candidate_metrics (
    candidate_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    status TEXT NOT NULL CHECK (status IN (
        'available',
        'insufficient_history',
        'missing_source',
        'market_date_mismatch',
        'valuation_unavailable',
        'analysis_failed'
    )),
    value REAL,
    previous_value REAL,
    delta REAL,
    unit TEXT NOT NULL CHECK (length(trim(unit)) > 0),
    as_of_date TEXT NOT NULL CHECK (
        length(as_of_date) = 10
        AND date(as_of_date) IS NOT NULL
        AND date(as_of_date) = as_of_date
    ),
    previous_as_of_date TEXT CHECK (
        previous_as_of_date IS NULL
        OR (
            length(previous_as_of_date) = 10
            AND date(previous_as_of_date) IS NOT NULL
            AND date(previous_as_of_date) = previous_as_of_date
        )
    ),
    observations INTEGER NOT NULL CHECK (observations >= 0),
    previous_observations INTEGER NOT NULL CHECK (previous_observations >= 0),
    PRIMARY KEY (candidate_id, ordinal),
    UNIQUE (candidate_id, name),
    FOREIGN KEY (candidate_id) REFERENCES screener_candidates(candidate_id)
        ON DELETE RESTRICT,
    CHECK (
        (status = 'available' AND value IS NOT NULL)
        OR (
            status <> 'available'
            AND value IS NULL
            AND previous_value IS NULL
            AND delta IS NULL
        )
    )
);

CREATE TABLE IF NOT EXISTS screener_source_artifacts (
    artifact_ref_id TEXT PRIMARY KEY CHECK (
        length(artifact_ref_id) = 64
        AND artifact_ref_id NOT GLOB '*[^0-9a-f]*'
    ),
    universe_run_id TEXT,
    screener_run_id TEXT,
    candidate_id TEXT,
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    source_role TEXT NOT NULL CHECK (
        source_role IN ('authority_input', 'canonical', 'validation')
    ),
    upstream_owner_kind TEXT,
    upstream_owner_run_id TEXT,
    provider TEXT NOT NULL CHECK (length(trim(provider)) > 0),
    dataset TEXT NOT NULL CHECK (length(trim(dataset)) > 0),
    source_ref TEXT NOT NULL CHECK (length(trim(source_ref)) > 0),
    contract_version TEXT NOT NULL CHECK (length(trim(contract_version)) > 0),
    payload_sha256 TEXT NOT NULL CHECK (
        length(payload_sha256) = 64
        AND payload_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    payload_size_bytes INTEGER NOT NULL CHECK (payload_size_bytes >= 0),
    hash_basis TEXT NOT NULL CHECK (
        hash_basis IN ('raw-response-bytes-v1', 'canonical-json-v1')
    ),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (universe_run_id) REFERENCES market_universe_runs(universe_run_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (screener_run_id) REFERENCES screener_runs(screener_run_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (candidate_id) REFERENCES screener_candidates(candidate_id)
        ON DELETE RESTRICT,
    CHECK (
        (universe_run_id IS NOT NULL)
        + (screener_run_id IS NOT NULL)
        + (candidate_id IS NOT NULL) = 1
    ),
    CHECK (
        (upstream_owner_kind IS NULL AND upstream_owner_run_id IS NULL)
        OR (
            upstream_owner_kind IS NOT NULL
            AND length(trim(upstream_owner_kind)) > 0
            AND upstream_owner_run_id IS NOT NULL
            AND length(trim(upstream_owner_run_id)) > 0
        )
    ),
    CHECK (
        universe_run_id IS NULL
        OR (source_role = 'authority_input' AND lower(provider) = 'twse')
    ),
    CHECK (
        source_role <> 'canonical'
        OR lower(provider) NOT IN ('esun', 'esun-historical')
    )
);

CREATE INDEX IF NOT EXISTS idx_market_universe_runs_market_date_status
    ON market_universe_runs(market_date, status);
CREATE INDEX IF NOT EXISTS idx_market_universe_members_symbol_status
    ON market_universe_members(symbol, status, universe_run_id);
CREATE INDEX IF NOT EXISTS idx_market_universe_members_run_status
    ON market_universe_members(universe_run_id, status, symbol);

CREATE INDEX IF NOT EXISTS idx_screener_runs_market_date_status
    ON screener_runs(market_date, status);
CREATE INDEX IF NOT EXISTS idx_screener_runs_methodology_date
    ON screener_runs(
        stage1_methodology_version, stage2_methodology_version, market_date
    );
CREATE INDEX IF NOT EXISTS idx_screener_candidates_symbol_run
    ON screener_candidates(symbol, screener_run_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_screener_candidates_run_rank_unique
    ON screener_candidates(screener_run_id, rank) WHERE rank IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_candidate_reasons_stage_code
    ON candidate_reasons(stage, code, candidate_id);
CREATE INDEX IF NOT EXISTS idx_candidate_reasons_metric
    ON candidate_reasons(metric, candidate_id);
CREATE INDEX IF NOT EXISTS idx_candidate_metrics_name_status
    ON candidate_metrics(name, status, candidate_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_screener_artifacts_universe_ordinal
    ON screener_source_artifacts(universe_run_id, ordinal)
    WHERE universe_run_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_screener_artifacts_run_ordinal
    ON screener_source_artifacts(screener_run_id, ordinal)
    WHERE screener_run_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_screener_artifacts_candidate_ordinal
    ON screener_source_artifacts(candidate_id, ordinal)
    WHERE candidate_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_screener_artifacts_payload_sha256
    ON screener_source_artifacts(payload_sha256);
CREATE INDEX IF NOT EXISTS idx_screener_artifacts_provider_dataset
    ON screener_source_artifacts(provider, dataset);

CREATE TRIGGER IF NOT EXISTS trg_market_universe_runs_success_terminal
BEFORE UPDATE OF status ON market_universe_runs
FOR EACH ROW
WHEN OLD.status = 'success' AND NEW.status <> OLD.status
BEGIN
    SELECT RAISE(ABORT, 'successful universe run is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_screener_runs_success_terminal
BEFORE UPDATE OF status ON screener_runs
FOR EACH ROW
WHEN OLD.status = 'success' AND NEW.status <> OLD.status
BEGIN
    SELECT RAISE(ABORT, 'successful screener run is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_screener_candidates_success_terminal
BEFORE UPDATE OF status ON screener_candidates
FOR EACH ROW
WHEN OLD.status = 'success' AND NEW.status <> OLD.status
BEGIN
    SELECT RAISE(ABORT, 'successful screener candidate is immutable');
END;
