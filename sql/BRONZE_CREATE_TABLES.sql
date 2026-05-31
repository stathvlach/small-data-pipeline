CREATE TABLE employees (
    dataset_hash TEXT NOT NULL,
    record_id INTEGER NOT NULL CHECK (record_id >= 1),
    employee_id TEXT,
    name TEXT,
    role TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (dataset_hash, record_id)
);

CREATE TABLE projects (
    dataset_hash TEXT NOT NULL,
    record_id INTEGER NOT NULL CHECK (record_id >= 1),
    project_id TEXT,
    project_name TEXT,
    budget TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (dataset_hash, record_id)
);

CREATE TABLE timesheets (
    dataset_hash TEXT NOT NULL,
    record_id INTEGER NOT NULL CHECK (record_id >= 1),
    employee_id TEXT,
    project_id TEXT,
    date TEXT,
    hours TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (dataset_hash, record_id)
);
