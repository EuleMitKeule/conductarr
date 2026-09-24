-- Jobs that conductarr itself paused (so it never resumes a job the user paused).
CREATE TABLE IF NOT EXISTS paused_jobs (
    nzo_id      TEXT PRIMARY KEY,
    paused_at   DATETIME NOT NULL DEFAULT (datetime('now'))
);

-- Every indexer search, used for search_interval / max_searches_per_day.
CREATE TABLE IF NOT EXISTS search_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    virtual_queue   TEXT NOT NULL,
    source          TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    outcome         TEXT NOT NULL DEFAULT '',
    searched_at     DATETIME NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_search_log_queue_time
    ON search_log (virtual_queue, searched_at);

-- Small persistent key/value store (candidate cursors, scan timestamps).
CREATE TABLE IF NOT EXISTS kv_state (
    key     TEXT PRIMARY KEY,
    value   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_queue_items_queue_source
    ON queue_items (virtual_queue, source);
