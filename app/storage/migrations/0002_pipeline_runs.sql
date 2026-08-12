CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    target_date TEXT NOT NULL,
    requested_start_date TEXT NOT NULL,
    requested_end_date TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'running', 'success', 'failed')
    ),
    provider TEXT NOT NULL,
    error_message TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    research_note_id INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (symbol, target_date),
    FOREIGN KEY (research_note_id) REFERENCES research_notes(id),
    CHECK (requested_start_date <= requested_end_date),
    CHECK (target_date = requested_end_date),
    CHECK (
        (status = 'success' AND finished_at IS NOT NULL
            AND research_note_id IS NOT NULL AND error_message IS NULL)
        OR (status = 'failed' AND finished_at IS NOT NULL
            AND research_note_id IS NULL AND error_message IS NOT NULL)
        OR (status IN ('pending', 'running') AND finished_at IS NULL
            AND research_note_id IS NULL AND error_message IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status_target
    ON pipeline_runs(status, target_date);
