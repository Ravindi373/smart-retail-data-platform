"""
dags/retail_pipeline.py

The single scheduled entry point for the platform. It runs the two
Airflow-managed layers in order and waits for each to finish:

    retail_bronze_ingestion  (Raw landing in MinIO + typed Bronze tables)
        -> retail_silver_clean  (validate, quarantine, hash PII, deduplicate)

Both child DAGs have no schedule of their own (they can still be triggered
manually from the UI to demo one layer in isolation); this DAG passes its
logical date down so every layer processes the same run date.

The Gold layer is built by dbt (`make gold`, i.e. `dbt build` in the dbt
container). dbt runs in its own container rather than inside Airflow to avoid
dependency conflicts between dbt and the pinned Airflow image - see the
"Known limitations" section of the report for the trade-off.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "retaillake",
    "retries": 0,
}


def _trigger(task_id, dag_id):
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag_id,
        logical_date="{{ logical_date }}",
        reset_dag_run=True,          # re-running a date replaces that run
        wait_for_completion=True,    # block until the child DAG finishes
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
        execution_timeout=timedelta(minutes=30),
    )


with DAG(
    dag_id="retail_pipeline",
    description="End-to-end run: Raw landing + Bronze, then Silver cleaning",
    default_args=default_args,
    schedule="@daily",
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retaillake", "orchestration"],
) as dag:
    bronze = _trigger("run_bronze_ingestion", "retail_bronze_ingestion")
    silver = _trigger("run_silver_clean", "retail_silver_clean")
    bronze >> silver
