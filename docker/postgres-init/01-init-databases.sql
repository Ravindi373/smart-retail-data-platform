-- Runs automatically on first Postgres container start (docker-entrypoint-initdb.d)

-- Separate database for Airflow's own metadata, kept apart from app data
CREATE DATABASE airflow;

-- Application database holding the medallion layers
CREATE DATABASE retaildb;

\connect retaildb

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS quality;

COMMENT ON SCHEMA bronze IS 'Typed source tables with ingestion metadata';
COMMENT ON SCHEMA silver IS 'Cleaned, deduplicated, PII-hashed data';
COMMENT ON SCHEMA gold IS 'Star schema facts and dimensions for BI';
COMMENT ON SCHEMA quality IS 'Quarantined rows and data quality summaries';

-- Audit log table used by ingestion DAGs (Week 3) to record each load
CREATE TABLE IF NOT EXISTS bronze.ingestion_audit_log (
    audit_id        SERIAL PRIMARY KEY,
    source_name     TEXT NOT NULL,
    file_path       TEXT NOT NULL,
    row_count       INTEGER NOT NULL,
    loaded_at       TIMESTAMP NOT NULL DEFAULT now(),
    run_date        DATE NOT NULL
);

-- Quarantine table used by quality checks (Week 4+)
CREATE TABLE IF NOT EXISTS quality.quarantine (
    quarantine_id   SERIAL PRIMARY KEY,
    source_name     TEXT NOT NULL,
    record_json     JSONB NOT NULL,
    failed_rule     TEXT NOT NULL,
    quarantined_at  TIMESTAMP NOT NULL DEFAULT now()
);
