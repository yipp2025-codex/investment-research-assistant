PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS symbols (
    symbol TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    market TEXT NOT NULL,
    currency TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_prices (
    symbol TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open_price REAL NOT NULL CHECK (open_price > 0),
    high_price REAL NOT NULL CHECK (high_price > 0),
    low_price REAL NOT NULL CHECK (low_price > 0),
    close_price REAL NOT NULL CHECK (close_price > 0),
    volume INTEGER NOT NULL CHECK (volume >= 0),
    source TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, trade_date),
    FOREIGN KEY (symbol) REFERENCES symbols(symbol) ON UPDATE CASCADE,
    CHECK (high_price >= open_price),
    CHECK (high_price >= low_price),
    CHECK (high_price >= close_price),
    CHECK (low_price <= open_price),
    CHECK (low_price <= high_price),
    CHECK (low_price <= close_price)
);

CREATE TABLE IF NOT EXISTS company_metrics (
    symbol TEXT NOT NULL,
    metric_date TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value REAL NOT NULL,
    unit TEXT,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, metric_date, metric_name),
    FOREIGN KEY (symbol) REFERENCES symbols(symbol) ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS research_notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    created_at TEXT NOT NULL,
    analysis_type TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    source_data_start TEXT NOT NULL,
    source_data_end TEXT NOT NULL,
    provider_source TEXT NOT NULL,
    FOREIGN KEY (symbol) REFERENCES symbols(symbol) ON UPDATE CASCADE,
    CHECK (source_data_start <= source_data_end)
);

CREATE INDEX IF NOT EXISTS idx_daily_prices_date
    ON daily_prices(trade_date);
CREATE INDEX IF NOT EXISTS idx_company_metrics_name_date
    ON company_metrics(metric_name, metric_date);
CREATE INDEX IF NOT EXISTS idx_research_notes_symbol_created
    ON research_notes(symbol, created_at DESC);
