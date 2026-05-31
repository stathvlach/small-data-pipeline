PRAGMA foreign_keys = ON;

CREATE TABLE employees (
    employee_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    role TEXT NOT NULL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE projects (
    project_id TEXT  PRIMARY KEY,
    project_name TEXT NOT NULL,
    budget INTEGER  CHECK (budget >= 0),
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE timesheets (
    employee_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    date DATE,
    hours REAL CHECK (hours >= 0 AND hours <= 24),
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (employee_id)
        REFERENCES employees(employee_id)
        ON DELETE RESTRICT,

    FOREIGN KEY (project_id)
        REFERENCES projects(project_id)
        ON DELETE RESTRICT,

    PRIMARY KEY (employee_id, project_id, date)
);

CREATE INDEX idx_timesheets_employee ON timesheets(employee_id);
CREATE INDEX idx_timesheets_project ON timesheets(project_id);
CREATE INDEX idx_timesheets_date ON timesheets(date);
