# Week 3-4 setup notes: Bronze ingestion + Silver cleaning DAGs

## What this adds

- `dags/retail_bronze_ingestion.py` — lands each of the 6 sample source files
  into MinIO under `raw/<source>/YYYY/MM/DD/`, then parses them into typed
  `bronze.*` Postgres tables with ingestion metadata columns.
- `dags/retail_silver_clean.py` — reads every Bronze table, validates each
  row, hashes PII, standardises timestamps, deduplicates by business key,
  and writes clean rows to `silver.*` tables. Anything that fails is written
  to `quality.quarantine` with a human-readable reason — never silently
  dropped.
- `src/db/db.py` — shared Postgres connection + idempotent `CREATE TABLE IF
  NOT EXISTS` for all Bronze/Silver tables (safe to run every DAG run).
- `src/connectors/storage.py` — MinIO upload helper, idempotent by content
  hash so re-running a DAG for the same date doesn't duplicate raw objects.
- `src/quality/rules.py` — the actual validation rules, unit-tested against
  the real generated dataset before being wired into the DAGs (see results
  below).

## Verified before deployment

Every rule and the full clean/quarantine flow was tested directly against
`data/sample/*` (not just written from assumption). Results:

| Source | Total rows | Clean | Quarantined |
|---|---|---|---|
| pos_sales | 5,050 | 4,792 | 258 (117 bad qty, 92 bad price, 49 duplicate) |
| customers | 1,010 | 973 | 37 (27 invalid email, ~10 duplicate) |
| ecommerce_orders | 3,000 | 2,946 | 54 (missing customer_id) |
| inventory_snapshots | 505 | 493 | 12 (7 null qty, 5 duplicate) |

Every row is accounted for (clean + quarantined = total) with no crashes
across the full dataset.

## Required docker-compose.yml changes

Three additions needed to `x-airflow-common` in your existing
`docker/docker-compose.yml`:

1. **Add Postgres app-database credentials** (DAGs connect to `retaildb`
   directly, separate from Airflow's own metadata DB connection):

```yaml
x-airflow-common: &airflow-common
  image: apache/airflow:2.9.3
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    USER: airflow
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/airflow
    AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY}
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__API__AUTH_BACKENDS: "airflow.api.auth.backend.basic_auth"
    MINIO_ENDPOINT: http://minio:9000
    MINIO_ROOT_USER: ${MINIO_ROOT_USER}
    MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    POSTGRES_USER: ${POSTGRES_USER}
    POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    _PIP_ADDITIONAL_REQUIREMENTS: "boto3"
```

(Only `boto3` needs adding — `psycopg2` is already present since Airflow
uses it for its own metadata database connection.)

2. **Mount the sample data folder** so the Bronze DAG can read it, alongside
   the existing `dags` and `src` mounts:

```yaml
  volumes:
    - ../dags:/opt/airflow/dags
    - airflow_logs:/opt/airflow/logs
    - ../src:/opt/airflow/src
    - ../data:/opt/airflow/data
```

3. **No other changes needed** — the webserver/scheduler service
  definitions, restart policy, and entrypoint fixes from Week 2 stay as-is.

Note: `_PIP_ADDITIONAL_REQUIREMENTS` installs boto3 on every container
start, which adds ~10-20 seconds to startup. This is fine for local
development; a production setup would instead build a custom image with
boto3 baked in.

## How to run

1. Copy `dags/`, `src/db/`, `src/connectors/storage.py`, and
   `src/quality/rules.py` into your repo at the matching paths.
2. Apply the three docker-compose.yml changes above.
3. Restart the stack:
   ```
   docker compose --env-file ../.env down
   docker compose --env-file ../.env up -d
   ```
4. Open the Airflow UI (http://localhost:8080), find
   `retail_bronze_ingestion`, and trigger it manually (the play button).
5. Once it succeeds, trigger `retail_silver_clean`.
6. Verify results in Postgres:
   ```
   docker exec -it retaillake-postgres psql -U retaillake -d retaildb -c "SELECT count(*) FROM bronze.pos_sales;"
   docker exec -it retaillake-postgres psql -U retaillake -d retaildb -c "SELECT count(*) FROM silver.pos_sales;"
   docker exec -it retaillake-postgres psql -U retaillake -d retaildb -c "SELECT source_name, failed_rule, count(*) FROM quality.quarantine GROUP BY 1,2 ORDER BY 1,2;"
   ```
7. Verify raw landing in the MinIO console (http://localhost:9001) — you
   should see `retail-raw/pos_sales/2026/...` and similar paths for each
   source.

## Idempotency (brief requirement: "running the same date twice does not
duplicate data")

- **Raw landing**: `storage.land_raw_file` checks the content hash of any
  existing object at the same key before uploading; identical re-runs are
  skipped, not re-uploaded.
- **Bronze**: each parse step deletes existing rows for the same
  `_run_date` before inserting the fresh batch, so re-running a date
  replaces rather than duplicates.
- **Silver**: uses a full-refresh (`TRUNCATE` + rebuild from all of
  Bronze) on every run. This is a deliberate simplification appropriate to
  this project's scale, documented in the DAG's docstring — a production
  system at larger scale would use incremental merge logic instead.

## Known simplification to mention in your final report

Since this is a local student project with no live upstream systems, "raw
landing" reads from the generated `data/sample/` files rather than polling
a real POS terminal, e-commerce API, etc. In production, the
`land_and_parse_*` functions' file-reading step would be replaced by a
real connector (database query, REST API call, SFTP pull) — the
landing/parsing/quarantine logic downstream of that point does not change.
