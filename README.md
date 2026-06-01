### Data Engineering Take-Home Assessment
# Small Data Pipeline
## Overview

This project implements a small data pipeline that ingests, validates, cleans, and transforms CSV datasets into analysis-ready outputs.

The solution follows a layered architecture inspired by modern data platform design principles:

CSV Files
    ↓
Bronze Layer
    ↓
Validation
    ↓
Cleaning
    ↓
Silver Layer
    ↓
Aggregation
    ↓
Gold Layer

Tracking Layer

The pipeline is designed to be rerunnable, auditable, and easy to extend.

---

## Project Structure

input/      -> Source CSV files
sql/        -> Database schemas
src/        -> Pipeline implementation
db/         -> SQLite databases (generated)
logs/       -> Execution logs (generated)

Bronze DB   -> Raw ingested data
Silver DB   -> Clean validated data
Gold DB     -> Aggregated outputs
Tracking DB -> Metadata, data quality and pipeline monitoring

---

## Data Model

### Bronze Layer

Stores raw records exactly as received from source files, including invalid and incomplete values.

#### Tables:

- employees
- projects
- timesheets

##### Primary key:

- dataset_hash, record_id

### Silver Layer

Stores cleaned and validated business entities.

#### Tables:

- employees
- projects
- timesheets

##### Key constraints:

- Primary Keys on business identifiers
- Foreign Keys between timesheets, employees and projects
- Range checks for hours and budget values

### Gold Layer

#### Stores analysis-ready aggregates.

##### Tables:

- total_hours_per_employee
- total_hours_per_project

### Tracking Layer

Stores operational metadata.

#### Tables:

- dataset_meta
- data_quality
- pipeline_tracker

## Validation Approach

Validation rules are executed against Bronze data before transformation.

Implemented checks include:

### Structural Validation

- Missing identifiers
- Invalid date formats
- Invalid naming patterns
- Non-numeric values

### Data Quality Validation

- Exact duplicate records
- Duplicate business keys
- Negative values
- Out-of-range hours worked

### Referential Integrity Validation

- Missing employee references
- Missing project references

Validation findings are stored in the "data_quality" table using two severity levels:

- ERROR → record rejected
- WARNING → record accepted but flagged for review

## Cleaning Approach

After validation, accepted records are cleaned through:

- Whitespace trimming
- Whitespace normalization
- Case normalization
- Numeric conversion
- Date standardization
- Foreign key filtering

Rejected records are tracked in the data quality repository for auditability.

---

## Outputs

### Clean Dataset

The Silver layer contains validated and cleaned business data.

### Aggregated Outputs

The Gold layer produces:

- Total hours per employee
- Total hours per project

## Re-runnability & Idempotency

The pipeline is safe to execute multiple times.

#### Implemented mechanisms:

- SHA256 dataset fingerprinting
- Dataset metadata tracking
- UPSERT-based loading using SQLite "ON CONFLICT"
- Deterministic business keys
- Pipeline execution tracking by run identifier

These controls prevent duplicate business records and provide execution traceability.

## Assumptions & Trade-offs

- Validation is intentionally stricter than cleaning; records with critical issues are rejected before transformation.
- Business-rule warnings are retained for review but do not necessarily block processing.
- SQLite was selected for simplicity and portability.
- The solution prioritizes clarity and maintainability over optimization.

## Future Improvements

Given additional time, the solution could be extended with:

- Airflow orchestration
- Automated data quality reports
- Configuration-driven validation rules
- Unit and integration tests
- Incremental processing strategies
- Data lineage and observability metrics

---

## Running the Pipeline

Install dependencies:
```
pip install -r requirements.txt
```
Execute the pipeline:
```
python src/main.py
```
The pipeline will:

1. Detect source CSV files
2. Load data into Bronze
3. Execute validation checks
4. Clean accepted records
5. Load Silver entities
6. Generate Gold aggregates
7. Store metadata, quality findings, and execution statistics
