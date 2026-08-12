CREATE TABLE IF NOT EXISTS market_data_validation_runs (
    run_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    target_date TEXT NOT NULL,
    requested_start_date TEXT NOT NULL,
    left_provider TEXT NOT NULL,
    right_provider TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'running', 'success', 'failed')
    ),
    outcome TEXT CHECK (outcome IN ('match', 'discrepancy')),
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    error_message TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    updated_at TEXT NOT NULL,
    UNIQUE (symbol, target_date, left_provider, right_provider),
    CHECK (left_provider <> right_provider),
    CHECK (requested_start_date <= target_date),
    CHECK (
        (status = 'success' AND finished_at IS NOT NULL
            AND outcome IS NOT NULL AND error_message IS NULL)
        OR (status = 'failed' AND finished_at IS NOT NULL
            AND outcome IS NULL AND error_message IS NOT NULL)
        OR (status IN ('pending', 'running') AND finished_at IS NULL
            AND outcome IS NULL AND error_message IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS market_data_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    symbol TEXT NOT NULL,
    market_date TEXT NOT NULL,
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
    UNIQUE (run_id, provider),
    FOREIGN KEY (run_id) REFERENCES market_data_validation_runs(run_id)
        ON DELETE CASCADE,
    CHECK (high_price >= open_price),
    CHECK (high_price >= low_price),
    CHECK (high_price >= close_price),
    CHECK (low_price <= open_price),
    CHECK (low_price <= high_price),
    CHECK (low_price <= close_price)
);

CREATE TABLE IF NOT EXISTS market_data_discrepancies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
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
    UNIQUE (run_id, field),
    FOREIGN KEY (run_id) REFERENCES market_data_validation_runs(run_id)
        ON DELETE CASCADE,
    CHECK (left_value IS NOT NULL OR right_value IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_market_validation_status_target
    ON market_data_validation_runs(status, target_date);
CREATE INDEX IF NOT EXISTS idx_market_validation_symbol_target
    ON market_data_validation_runs(symbol, target_date);
CREATE INDEX IF NOT EXISTS idx_market_observations_symbol_date
    ON market_data_observations(symbol, market_date, provider);
