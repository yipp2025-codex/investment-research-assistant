PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS daily_research_results (
    result_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL REFERENCES symbols(symbol),
    market_date TEXT NOT NULL,
    requested_date TEXT NOT NULL,
    methodology_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    result_status TEXT NOT NULL DEFAULT 'success'
        CHECK (result_status = 'success'),
    data_quality_status TEXT NOT NULL
        CHECK (data_quality_status IN ('clean', 'warning', 'blocked', 'unavailable')),
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (length(payload_sha256) = 64),
    provenance_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (symbol, market_date, methodology_version)
);

CREATE TABLE IF NOT EXISTS daily_research_reports (
    report_id TEXT PRIMARY KEY,
    result_id TEXT NOT NULL,
    symbol TEXT NOT NULL REFERENCES symbols(symbol),
    market_date TEXT NOT NULL,
    methodology_version TEXT NOT NULL,
    report_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (report_status IN ('pending', 'rendered', 'failed')),
    markdown TEXT,
    markdown_sha256 TEXT CHECK (
        markdown_sha256 IS NULL OR length(markdown_sha256) = 64
    ),
    render_error TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (result_id),
    UNIQUE (symbol, market_date, methodology_version),
    FOREIGN KEY (result_id) REFERENCES daily_research_results(result_id)
        ON DELETE CASCADE,
    CHECK (
        (report_status = 'rendered' AND markdown IS NOT NULL
            AND markdown_sha256 IS NOT NULL AND render_error IS NULL)
        OR (report_status = 'failed' AND markdown IS NULL
            AND markdown_sha256 IS NULL AND render_error IS NOT NULL)
        OR (report_status = 'pending' AND markdown IS NULL
            AND markdown_sha256 IS NULL AND render_error IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_daily_research_results_symbol_date
    ON daily_research_results(symbol, methodology_version, market_date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_research_reports_status
    ON daily_research_reports(report_status, updated_at DESC);
