# Week 7 setup notes: end-to-end pipeline, quality gate, reliability docs

This addresses four specific review points directly:

1. **"End-to-End Pipeline should be demonstrated as one integrated
   flow"** → `dags/retail_full_pipeline.py`, a single DAG chaining
   Bronze → Silver → Gold (dbt) → dbt tests → a quality gate.
2. **"Data Quality Handling should be clearly demonstrated"** →
   `docs/data_quality_report.md`, real verified numbers tying rules to
   results to what happens to a failing row.
3. **"Gold Layer tables need clear grain, purpose, business meaning"** →
   `docs/gold_data_dictionary.md`, a standalone document (not just
   buried in dbt YAML).
4. **"Pipeline Reliability — failures, reruns, unexpected data, not just
   successful execution"** → `docs/pipeline_reliability.md` plus the
   `quality_gate` task itself, which can fail a pipeline run even when
   every individual task reports success.

## What this adds

- `dags/retail_full_pipeline.py` — the master orchestrator DAG.
- `docs/gold_data_dictionary.md`
- `docs/data_quality_report.md`
- `docs/pipeline_reliability.md`

## Why the master DAG needs new Docker Compose changes

`retail_full_pipeline` runs `dbt run` and `dbt test` as Bash commands
*inside the Airflow container* (not by calling out to the separate
`dbt` service). This is a deliberate, simpler choice over having
Airflow talk to the `dbt` container via the Docker socket
(Docker-in-Docker), which is a well-known source of Windows/WSL2
permission problems — exactly the kind this project already hit twice
with pip installs into the wrong Python environment. Installing
dbt-core directly into Airflow's own Python environment avoids that
entire class of problem.

## Docker Compose changes needed

In `docker/docker-compose.yml`, update the `x-airflow-common` block:

**1. Extend `_PIP_ADDITIONAL_REQUIREMENTS`** to include dbt:

```yaml
    _PIP_ADDITIONAL_REQUIREMENTS: "boto3 dbt-core dbt-postgres"
```

**2. Add two new volume mounts**, alongside the existing ones:

```yaml
  volumes:
    - ../dags:/opt/airflow/dags
    - airflow_logs:/opt/airflow/logs
    - ../src:/opt/airflow/src
    - ../data:/opt/airflow/data
    - ../dbt_smart_retail:/opt/airflow/dbt_smart_retail
    - ./dbt-profiles:/opt/airflow/dbt-profiles
```

(`./dbt-profiles` is the same folder you already created for the
standalone `dbt` service in Week 5 — it's being reused here, not
duplicated.)

No other changes needed — the existing `postgres`, `minio`, `dbt`, and
`superset` services stay exactly as they are. The standalone `dbt`
service remains useful for ad hoc `dbt run --select <model>` during
development; `retail_full_pipeline` is for the full, single-trigger
demonstration run.

## How to run

1. Apply the two Docker Compose changes above.
2. Copy `dags/retail_full_pipeline.py` and the three new `docs/*.md`
   files into your repo at the matching paths.
3. Restart the stack so the new pip packages and volume mounts take
   effect:
   ```
   docker compose --env-file ../.env down
   docker compose --env-file ../.env up -d
   ```
   This will take noticeably longer than usual on first start — the
   `_PIP_ADDITIONAL_REQUIREMENTS` install now includes dbt-core and
   dbt-postgres, which are larger than boto3 alone.
4. In the Airflow UI, find `retail_full_pipeline`, unpause it, and
   trigger it manually.
5. Watch it run through all five tasks in order:
   `ensure_bronze_silver_tables` → `run_bronze_ingestion` →
   `run_silver_clean` → `dbt_run_gold` → `dbt_test_gold` →
   `quality_gate`. The two `TriggerDagRunOperator` tasks will show as
   running for as long as the underlying `retail_bronze_ingestion` /
   `retail_silver_clean` DAGs take to finish — this is expected, not
   stuck.
6. Confirm final state:
   ```
   docker exec -it retaillake-postgres psql -U retaillake -d retaildb -c "SELECT count(*) FROM gold.fact_sales_transaction;"
   ```

## Testing reliability (do this once, screenshot it)

Both tests are described in full in `docs/pipeline_reliability.md`
section 4 — a rerun-safety test (trigger the pipeline twice, confirm
row counts don't change) and a deliberate-failure test (truncate a
Silver table, confirm `quality_gate` catches it with a clear error
message even though upstream tasks all reported success). The second
test in particular is strong, screenshot-able evidence that this
pipeline is evaluated on data correctness, not just task exit codes.

## Evidence to capture for your mentor sync

- Full Grid view of a successful `retail_full_pipeline` run, all 6
  tasks green, in one screenshot — this is the single "yes, this is
  one integrated end-to-end pipeline" artifact.
- Terminal output of the deliberate-failure test showing
  `quality_gate` raising its error message.
- `gold_data_dictionary.md`, `data_quality_report.md`, and
  `pipeline_reliability.md` themselves are documentation evidence —
  no screenshot needed, just reference them directly.
