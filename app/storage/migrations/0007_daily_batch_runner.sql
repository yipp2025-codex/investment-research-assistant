PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS watchlists (
    watchlist_id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS watchlist_members (
    watchlist_id TEXT NOT NULL REFERENCES watchlists(watchlist_id),
    symbol TEXT NOT NULL REFERENCES symbols(symbol),
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    added_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (watchlist_id, symbol)
);

CREATE TABLE IF NOT EXISTS watchlist_revisions (
    revision_id TEXT PRIMARY KEY,
    watchlist_id TEXT NOT NULL REFERENCES watchlists(watchlist_id),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS watchlist_revision_members (
    revision_id TEXT NOT NULL REFERENCES watchlist_revisions(revision_id),
    symbol TEXT NOT NULL REFERENCES symbols(symbol),
    PRIMARY KEY (revision_id, symbol)
);

CREATE TABLE IF NOT EXISTS daily_batch_runs (
    batch_run_id TEXT PRIMARY KEY,
    watchlist_revision_id TEXT NOT NULL REFERENCES watchlist_revisions(revision_id),
    requested_date TEXT NOT NULL,
    resolved_market_date TEXT,
    runner_policy_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN (
            'pending', 'running', 'success', 'partial_success', 'failed',
            'skipped_non_trading_day', 'skipped_no_new_market_date',
            'deferred_awaiting_market_data'
        )),
    total_symbols INTEGER NOT NULL DEFAULT 0,
    success_symbols INTEGER NOT NULL DEFAULT 0,
    failed_symbols INTEGER NOT NULL DEFAULT 0,
    skipped_symbols INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (watchlist_revision_id, requested_date, runner_policy_version)
);

CREATE TABLE IF NOT EXISTS daily_symbol_runs (
    symbol_run_id TEXT PRIMARY KEY,
    batch_run_id TEXT NOT NULL REFERENCES daily_batch_runs(batch_run_id),
    symbol TEXT NOT NULL REFERENCES symbols(symbol),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN (
            'pending', 'running', 'success', 'failed',
            'skipped_already_succeeded', 'skipped_non_trading_day',
            'skipped_no_new_market_date'
        )),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    pipeline_run_id TEXT,
    historical_run_id TEXT,
    validation_run_id TEXT,
    hist_validation_run_id TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (batch_run_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_daily_batch_runs_requested_date
    ON daily_batch_runs(requested_date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_symbol_runs_batch_status
    ON daily_symbol_runs(batch_run_id, status);
CREATE INDEX IF NOT EXISTS idx_daily_symbol_runs_symbol_status
    ON daily_symbol_runs(symbol, status);
