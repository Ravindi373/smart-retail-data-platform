"""
dags/retail_full_pipeline.py

The single, end-to-end RetailLake pipeline. This is the DAG a mentor or
reviewer should trigger to see the whole platform run as ONE integrated
flow, rather than three independently-triggered pieces:

    ensure tables
        -> retail_bronze_ingestion   (land raw + parse Bronze)
        -> retail_silver_clean       (validate, hash, quarantine, Silver)
        -> dbt run                   (build the Gold star schema)
        -> dbt test                  (27 dbt tests: not_null/unique/relationships)
        -> quality_gate              (fail the run if data looks structurally wrong)

Why a separate orchestrator instead of merging everything into one giant
DAG file: retail_bronze_ingestion and retail_silver_clean remain useful
and testable on their own (e.g. re-running just Silver after a rule
change, without re-landing raw files). This DAG composes them via
TriggerDagRunOperator rather than duplicating their logic, so there is
exactly one definition of "how Bronze ingestion works" — this DAG only
decides the *order* things run in and *whether the result is good enough
to call successful*.

Reliability note: TriggerDagRunOperator with wait_for_completion=True
means this DAG's own success/failure genuinely reflects whether Bronze
and Silver succeeded — a failure partway through (e.g. Bronze fails)
correctly stops the pipeline here rather than silently continuing to
build Gold from stale or incomplete Silver data.
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.insert(0, "/opt/airflow/src")
from db.db import get_conn, ensure_tables  # noqa: E402

DBT_PROJECT_DIR = "/opt/airflow/dbt_smart_retail"
DBT_PROFILES_DIR = "/opt/airflow/dbt-profiles"

# Sources expected in Bronze — every one of these must have at least one
# row, or ingestion has silently failed for that source.
BRONZE_TABLES = [
    "bronze.pos_sales", "bronze.customers", "bronze.products",
    "bronze.inventory_snapshots", "bronze.ecommerce_orders",
    "bronze.supplier_deliveries",
]

# Sources expected in Silver — every one of these must have at least one
# CLEAN row, or the validation rules rejected everything (a much more
# serious failure mode than "some rows were invalid").
SILVER_TABLES = [
    "silver.pos_sales", "silver.customers", "silver.products",
    "silver.inventory_snapshots", "silver.ecommerce_orders",
    "silver.supplier_deliveries",
]

# Maximum acceptable quarantine rate per source before the pipeline
# refuses to call itself successful. Set conservatively above the
# dataset's known ~1-5% injected bad-data rate (see
# docs/gold_data_dictionary.md and the Week 4-5 sync notes) so normal
# runs pass comfortably, but a real upstream data regression (e.g. a
# broken source system suddenly sending 60% garbage) gets caught here
# rather than silently accepted.
MAX_QUARANTINE_RATE = 0.15  # 15%


def _ensure_tables(**_):
    ensure_tables()


def _quality_gate(**_):
    """Fails the task (and therefore the whole pipeline run) if the data
    looks structurally wrong, rather than just checking that tasks
    exited with code 0. This is the concrete answer to 'the pipeline
    should be evaluated on how it handles failures and unexpected data,
    not just successful execution' — a run that completes every task
    but produces an empty Silver table, or a source that's suddenly 90%
    quarantined, is NOT a successful run, and this task says so."""
    conn = get_conn()
    problems = []

    try:
        with conn.cursor() as cur:
            # 1. Every Bronze table must have landed at least one row.
            for table in BRONZE_TABLES:
                cur.execute(f"SELECT count(*) FROM {table}")
                count = cur.fetchone()[0]
                if count == 0:
                    problems.append(f"{table} is EMPTY — ingestion silently failed")

            # 2. Every Silver table must have at least one CLEAN row.
            for table in SILVER_TABLES:
                cur.execute(f"SELECT count(*) FROM {table}")
                count = cur.fetchone()[0]
                if count == 0:
                    problems.append(
                        f"{table} is EMPTY — every row was quarantined "
                        f"or the cleaning step failed"
                    )

            # 3. Per-source quarantine rate must stay under threshold.
            cur.execute(
                """SELECT source_name, sum(quarantined_count)
                   FROM gold.quality_summary GROUP BY source_name"""
            )
            quarantine_by_source = dict(cur.fetchall())

            for bronze_table in BRONZE_TABLES:
                source_name = bronze_table.split(".")[1]
                cur.execute(f"SELECT count(*) FROM {bronze_table}")
                bronze_count = cur.fetchone()[0]
                quarantined = quarantine_by_source.get(source_name, 0)
                if bronze_count == 0:
                    continue  # already flagged above
                rate = quarantined / bronze_count
                if rate > MAX_QUARANTINE_RATE:
                    problems.append(
                        f"{source_name}: {rate:.1%} of rows quarantined "
                        f"(threshold {MAX_QUARANTINE_RATE:.0%}) — "
                        f"possible upstream data quality regression"
                    )
    finally:
        conn.close()

    if problems:
        raise ValueError(
            "Quality gate FAILED — pipeline run is not considered "
            "successful despite all upstream tasks completing:\n  - "
            + "\n  - ".join(problems)
        )

    print("Quality gate PASSED — Bronze, Silver, and quarantine rates "
          "all within expected bounds.")


default_args = {
    "owner": "retaillake",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retail_full_pipeline",
    description=(
        "Single end-to-end pipeline: Bronze ingestion -> Silver cleaning "
        "-> Gold (dbt) -> dbt tests -> quality gate"
    ),
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retaillake", "end-to-end"],
) as dag:

    ensure_tables_task = PythonOperator(
        task_id="ensure_bronze_silver_tables",
        python_callable=_ensure_tables,
    )

    run_bronze = TriggerDagRunOperator(
        task_id="run_bronze_ingestion",
        trigger_dag_id="retail_bronze_ingestion",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=10,
        failed_states=["failed"],
    )

    run_silver = TriggerDagRunOperator(
        task_id="run_silver_clean",
        trigger_dag_id="retail_silver_clean",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=10,
        failed_states=["failed"],
    )

    dbt_run = BashOperator(
        task_id="dbt_run_gold",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test_gold",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
        # dbt exits non-zero only on ERROR-severity test failures, not on
        # WARN — so the documented 339-row cross-table finding (Week 5)
        # does not fail this task. A genuine new test failure will.
    )

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=_quality_gate,
    )

    (
        ensure_tables_task
        >> run_bronze
        >> run_silver
        >> dbt_run
        >> dbt_test
        >> quality_gate
    )
