CREATE TABLE IF NOT EXISTS historical_sync_runs (
    run_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    target_date TEXT NOT NULL,
    target_observations INTEGER NOT NULL CHECK (
        target_observations BETWEEN 60 AND 250
    ),
    provider TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'running', 'success', 'failed')
    ),
    next_month TEXT NOT NULL,
    months_completed INTEGER NOT NULL DEFAULT 0 CHECK (months_completed >= 0),
    observation_count INTEGER NOT NULL DEFAULT 0 CHECK (observation_count >= 0),
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    error_message TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    research_note_id INTEGER,
    source_endpoint TEXT,
    fetched_at TEXT,
    first_trade_date TEXT,
    last_trade_date TEXT,
    updated_at TEXT NOT NULL,
    UNIQUE (symbol, target_date, target_observations, provider),
    FOREIGN KEY (research_note_id) REFERENCES research_notes(id),
    CHECK (
        (status = 'success' AND finished_at IS NOT NULL
            AND research_note_id IS NOT NULL AND error_message IS NULL)
        OR (status = 'failed' AND finished_at IS NOT NULL
            AND research_note_id IS NULL AND error_message IS NOT NULL)
        OR (status IN ('pending', 'running') AND finished_at IS NULL
            AND research_note_id IS NULL AND error_message IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_historical_sync_status_target
    ON historical_sync_runs(status, target_date);
CREATE INDEX IF NOT EXISTS idx_historical_sync_symbol_target
    ON historical_sync_runs(symbol, target_date);
