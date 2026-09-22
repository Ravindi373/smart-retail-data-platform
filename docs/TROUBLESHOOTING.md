# Troubleshooting

Format: **symptom** -> cause -> fix. Items marked *(happened during the build)*
are problems hit and solved while building this project.

## Startup

**Airflow webserver dies or never becomes reachable** *(happened during the build)*
-> the default gunicorn workers need more RAM than Docker/WSL2 had.
-> the Compose file runs `airflow webserver --debug` (bypasses gunicorn) with `restart: unless-stopped`. If it still restarts, give Docker Desktop / WSL2 more memory (`%UserProfile%\.wslconfig`, `memory=6GB`, then `wsl --shutdown`).

**Superset shows "database is locked" or fails to start** *(happened during the build)*
-> SQLite as Superset's metadata database locks under Docker/WSL2.
-> Superset's metadata lives in Postgres (`docker/superset-config/superset_config.py`, database `superset`). The database is created by `docker/postgres-init/01-init-databases.sql` **only on the first start of an empty Postgres volume**. If you already have a volume: `docker exec -it retaillake-postgres psql -U <user> -c "CREATE DATABASE superset;"`, then restart Superset.

**Superset: `No module named psycopg2`** *(happened during the build)*
-> the image's virtualenv has no Postgres driver and its files are read-only for the default user.
-> the Compose `superset` service runs as root and installs `psycopg2-binary` with `uv` before starting. Keep that command.

**`ModuleNotFoundError: boto3` in a DAG right after `docker compose up`**
-> `_PIP_ADDITIONAL_REQUIREMENTS` installs boto3 each time the container starts (10-20 s).
-> wait until the scheduler log stops installing, then retry.

**`address already in use` for 5432 / 8080 / 8088 / 9000 / 9001**
-> another program (often a local Postgres) uses the port.
-> stop it, or change the left-hand port in `docker-compose.yml`.

**MinIO / Airflow `InvalidAccessKeyId` or 403**
-> `.env` credentials changed after the containers were created.
-> `docker compose --env-file .env -f docker/docker-compose.yml up -d --force-recreate`.

## Pipeline

**`retail_pipeline` sits on `run_bronze_ingestion` forever**
-> a child DAG is paused, so the triggered run is created but never scheduled.
-> `make unpause` (or un-pause `retail_bronze_ingestion` and `retail_silver_clean` in the UI). New installs create DAGs unpaused.

**Silver log: `Bronze returned 0 rows - has retail_bronze_ingestion run?`**
-> Silver ran before Bronze, or Bronze was wiped.
-> run `make bronze` first.

**Silver log: `no source extract time found - using current time`**
-> `data/sample/_manifest.json` is missing, so "future-dated" is measured against today and results change over time.
-> `make seed` (regenerates the manifest) or set `SOURCE_EXTRACT_TS=2026-08-23T12:00:00` for the Airflow containers.

**Raw log: `content changed ... original kept, new version landed as ...`**
-> you regenerated the source files and re-ran the **same date**. Raw is never overwritten, so the new content sits beside the original as `<name>.<hash>.<ext>`. Expected, not an error. Bronze always parses the current file.

**Quarantine has thousands of `duplicate_business_key` rows, or more quarantined rows than source rows** *(happened during the build, twice)*
-> (1) `quality.quarantine` was never cleared between runs; (2) Silver read every Bronze load, so a second date doubled the data.
-> both are fixed: Silver clears its quality tables at the start of each run and reads only the latest Bronze load. If it reappears, check the `WHERE _run_date = (SELECT max(_run_date) ...)` filter in `dags/retail_silver_clean.py`.

**A DAG task fails writing to `quality.quarantine` with a JSON error** *(happened during the build)*
-> NUMERIC columns come back as Python `Decimal`, which JSON cannot encode.
-> `_json_safe()` in `retail_silver_clean.py` converts Decimal/date values first; there is a unit test for it.

**Bronze parsed a JSON file as CSV / column-name mismatch** *(happened during the build)*
-> `ecommerce_orders` and `supplier_deliveries` are JSON and have their own handlers; CSV column names must match the Bronze DDL exactly (`timestamp` -> `raw_timestamp`).
-> see `SOURCES` and the two dedicated handlers in `dags/retail_bronze_ingestion.py`.

## dbt

**`WARN ... relationships_fact_sales_transaction_customer_key` (338 rows)**
-> expected. It reports the documented customer gap (see `docs/data_quality.md`). Not a failure.

**`not_null_dim_store_region` fails**
-> a store id appears in the sales or inventory data but not in the store master.
-> check `data/sample/stores.csv`, `bronze.stores`, `silver.stores`.

**`assert_quarantine_accounts_for_every_row` fails**
-> quarantine and `quality.run_summary` disagree, usually because Silver was only partly re-run. Re-run `make silver`.

**`dbt debug` cannot connect**
-> the dbt container needs `POSTGRES_*` from `.env`; recreate it with `--force-recreate`.

## Superset

**A new column (for example `region`) is missing in a chart or filter**
-> the dataset caches its column list.
-> Data -> Datasets -> the dataset -> Edit -> **Sync columns from source**.

## CI (GitHub Actions)

**`ruff` fails** -> run `make lint` locally and fix what it prints.
**`e2e` fails** -> open the run, then *Summary -> Artifacts -> pipeline-evidence* for dbt results and the S3 emulator log. Reproduce locally with the same environment variables (see `tests/e2e/test_pipeline_e2e.py`).
**"No secrets committed" fails** -> `.env` is tracked or `.env.example` holds a real-looking secret. Remove it, **rotate the secret**, and remember Git history keeps the old value.

**`test_committed_sample_data_matches_the_generator` fails, but only on some machines**
-> Python 3.13+ produces different Faker output than 3.11/3.12 even with the same seed - a real cross-version reproducibility gap in Python's own random number generator, not a bug in this project's code.
-> generate `data/sample/` using Python 3.11 or 3.12 specifically: `py -3.12 scripts\seed_mock_sources.py --seed 42 --out-dir data\sample` (Windows) or `python3.12 scripts/seed_mock_sources.py --seed 42 --out-dir data/sample` (Mac/Linux). Verify with `pip show Faker` that it is pinned to 26.0.0 as well - both the interpreter version and the Faker version must match for byte-identical output.

