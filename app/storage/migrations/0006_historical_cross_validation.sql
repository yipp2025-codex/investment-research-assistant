CREATE TABLE IF NOT EXISTS historical_source_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    historical_run_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open_price REAL NOT NULL CHECK (open_price > 0),
    high_price REAL NOT NULL CHECK (high_price > 0),
    low_price REAL NOT NULL CHECK (low_price > 0),
    close_price REAL NOT NULL CHECK (close_price > 0),
    volume INTEGER NOT NULL CHECK (volume >= 0),
    source_endpoints_json TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    source_timestamp_raw TEXT,
    source_timestamp TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (historical_run_id, trade_date),
    FOREIGN KEY (historical_run_id) REFERENCES historical_sync_runs(run_id)
        ON DELETE CASCADE,
    CHECK (high_price >= open_price),
    CHECK (high_price >= low_price),
    CHECK (high_price >= close_price),
    CHECK (low_price <= open_price),
    CHECK (low_price <= high_price),
    CHECK (low_price <= close_price)
);

CREATE TABLE IF NOT EXISTS historical_validation_runs (
    run_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    target_date TEXT NOT NULL,
    target_observations INTEGER NOT NULL CHECK (
        target_observations BETWEEN 60 AND 250
    ),
    left_provider TEXT NOT NULL,
    right_provider TEXT NOT NULL,
    left_historical_run_id TEXT NOT NULL,
    right_historical_run_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'running', 'success', 'failed')
    ),
    outcome TEXT CHECK (outcome IN ('match', 'discrepancy')),
    common_date_count INTEGER NOT NULL DEFAULT 0 CHECK (common_date_count >= 0),
    matched_date_count INTEGER NOT NULL DEFAULT 0 CHECK (matched_date_count >= 0),
    left_only_date_count INTEGER NOT NULL DEFAULT 0 CHECK (
        left_only_date_count >= 0
    ),
    right_only_date_count INTEGER NOT NULL DEFAULT 0 CHECK (
        right_only_date_count >= 0
    ),
    field_discrepancy_count INTEGER NOT NULL DEFAULT 0 CHECK (
        field_discrepancy_count >= 0
    ),
    left_latest_date TEXT,
    right_latest_date TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    error_message TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    research_note_id INTEGER,
    updated_at TEXT NOT NULL,
    UNIQUE (left_historical_run_id, right_historical_run_id),
    FOREIGN KEY (left_historical_run_id) REFERENCES historical_sync_runs(run_id),
    FOREIGN KEY (right_historical_run_id) REFERENCES historical_sync_runs(run_id),
    FOREIGN KEY (research_note_id) REFERENCES research_notes(id),
    CHECK (left_provider <> right_provider),
    CHECK (
        (status = 'success' AND finished_at IS NOT NULL
            AND outcome IS NOT NULL AND research_note_id IS NOT NULL
            AND error_message IS NULL)
        OR (status = 'failed' AND finished_at IS NOT NULL
            AND outcome IS NULL AND research_note_id IS NULL
            AND error_message IS NOT NULL)
        OR (status IN ('pending', 'running') AND finished_at IS NULL
            AND outcome IS NULL AND research_note_id IS NULL
            AND error_message IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS historical_validation_discrepancies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    field TEXT NOT NULL CHECK (length(trim(field)) > 0),
    left_value TEXT,
    right_value TEXT,
    reason TEXT NOT NULL CHECK (length(trim(reason)) > 0),
    absolute_difference REAL CHECK (
        absolute_difference IS NULL OR absolute_difference >= 0
    ),
    relative_difference_pct REAL CHECK (
        relative_difference_pct IS NULL OR relative_difference_pct >= 0
    ),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id, trade_date, field),
    FOREIGN KEY (run_id) REFERENCES historical_validation_runs(run_id)
        ON DELETE CASCADE,
    CHECK (left_value IS NOT NULL OR right_value IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_historical_source_run_date
    ON historical_source_observations(historical_run_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_historical_source_symbol_provider_date
    ON historical_source_observations(symbol, provider, trade_date);
CREATE INDEX IF NOT EXISTS idx_historical_validation_status_target
    ON historical_validation_runs(status, target_date);
CREATE INDEX IF NOT EXISTS idx_historical_validation_symbol_target
    ON historical_validation_runs(symbol, target_date);
