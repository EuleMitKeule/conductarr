CREATE TABLE IF NOT EXISTS schema_versions (
    version     INTEGER PRIMARY KEY,
    applied_at  DATETIME NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS queue_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    virtual_queue   TEXT,
    tags            TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_tried_at   DATETIME,
    created_at      DATETIME NOT NULL DEFAULT (datetime('now')),
    updated_at      DATETIME NOT NULL DEFAULT (datetime('now')),
    metadata        TEXT NOT NULL DEFAULT '{}',
    UNIQUE(source, source_id)
);

CREATE TABLE IF NOT EXISTS sabnzbd_job_map (
    nzo_id          TEXT PRIMARY KEY,
    queue_item_id   INTEGER REFERENCES queue_items(id) ON DELETE SET NULL,
    virtual_queue   TEXT,
    detected_at     DATETIME NOT NULL DEFAULT (datetime('now'))
);
