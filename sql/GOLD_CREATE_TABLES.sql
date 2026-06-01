CREATE TABLE total_hours_per_project (
    project_id TEXT PRIMARY KEY,
    total_hours REAL NOT NULL CHECK (total_hours >= 0)
);

CREATE TABLE total_hours_per_employee (
    employee_id TEXT PRIMARY KEY,
    total_hours REAL NOT NULL CHECK (total_hours >= 0)
);
