PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS scheduler_invocations (
    invocation_id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    trigger TEXT NOT NULL CHECK (trigger IN ('manual', 'scheduled')),
    mode TEXT NOT NULL CHECK (mode = 'eod'),
    requested_date TEXT NOT NULL,
    scheduled_for TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN (
            'pending', 'running', 'success', 'warning', 'partial_failure',
            'hard_failure', 'skip', 'deferred'
        )),
    batch_run_id TEXT REFERENCES daily_batch_runs(batch_run_id),
    process_id INTEGER,
    started_at TEXT,
    finished_at TEXT,
    next_retry_at TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_scheduler_invocations_job_date
    ON scheduler_invocations(job_name, requested_date DESC, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_scheduler_invocations_status
    ON scheduler_invocations(status, next_retry_at);

CREATE TABLE IF NOT EXISTS operation_leases (
    lease_id TEXT PRIMARY KEY,
    lease_key TEXT NOT NULL,
    invocation_id TEXT NOT NULL REFERENCES scheduler_invocations(invocation_id),
    status TEXT NOT NULL CHECK (status IN ('active', 'released', 'expired')),
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    released_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operation_leases_status_expiry
    ON operation_leases(status, expires_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_operation_leases_active_key
    ON operation_leases(lease_key) WHERE status = 'active';

CREATE TABLE IF NOT EXISTS operation_logs (
    operation_log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    invocation_id TEXT NOT NULL REFERENCES scheduler_invocations(invocation_id),
    level TEXT NOT NULL CHECK (level IN ('info', 'warning', 'error')),
    event TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operation_logs_invocation
    ON operation_logs(invocation_id, created_at, operation_log_id);
