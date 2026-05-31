CREATE TABLE dataset_meta (
    dataset_hash TEXT PRIMARY KEY,
    filename TEXT,
    first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
    last_seen TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE data_quality (
    table_name TEXT,
    dataset_hash TEXT,
    record_id INTEGER,
    severity TEXT NOT NULL CHECK (severity IN ('ERROR', 'WARNING')),
    issue TEXT NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (table_name, dataset_hash, record_id)
);

CREATE TABLE pipeline_tracker (
    run_id INTEGER NOT NULL,
    step TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_name TEXT NOT NULL,
    obs INTEGER NOT NULL,
    csv_dataset_hash TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);
