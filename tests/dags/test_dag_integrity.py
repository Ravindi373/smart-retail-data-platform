"""DAG integrity tests: every DAG parses, has the expected shape, and wires
its layers in the right order. Needs apache-airflow installed."""
import decimal
import os
from datetime import date, datetime
from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

pytestmark = pytest.mark.dags
DAGS = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    os.environ.setdefault("POSTGRES_USER", "x")
    os.environ.setdefault("POSTGRES_PASSWORD", "x")
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_all_dags_import_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_exist(dagbag):
    assert {"retail_pipeline", "retail_bronze_ingestion", "retail_silver_clean"} <= set(dagbag.dag_ids)


def test_only_the_master_dag_is_scheduled(dagbag):
    assert dagbag.get_dag("retail_pipeline").schedule_interval == "@daily"
    assert dagbag.get_dag("retail_bronze_ingestion").schedule_interval is None
    assert dagbag.get_dag("retail_silver_clean").schedule_interval is None


def test_master_runs_bronze_before_silver(dagbag):
    dag = dagbag.get_dag("retail_pipeline")
    assert dag.get_task("run_silver_clean").upstream_task_ids == {"run_bronze_ingestion"}


def test_bronze_has_a_task_for_every_source(dagbag):
    ids = dagbag.get_dag("retail_bronze_ingestion").task_ids
    for source in ["pos_sales", "customers", "products", "stores", "inventory_snapshots",
                   "ecommerce_orders", "supplier_deliveries"]:
        assert f"land_and_parse.land_and_parse_{source}" in ids


def test_silver_cleans_products_before_anything_that_resolves_product_ids(dagbag):
    dag = dagbag.get_dag("retail_silver_clean")
    for dependent in ["clean_pos_sales", "clean_ecommerce_orders", "clean_inventory_snapshots"]:
        assert "clean_products" in dag.get_task(dependent).upstream_task_ids


def test_silver_quality_report_runs_last(dagbag):
    dag = dagbag.get_dag("retail_silver_clean")
    assert dag.get_task("log_quality_report").downstream_task_ids == set()
    assert len(dag.get_task("log_quality_report").upstream_task_ids) == 7


def test_every_dag_has_an_owner_and_tags(dagbag):
    for dag in dagbag.dags.values():
        assert dag.default_args["owner"] == "retaillake"
        assert dag.tags


def test_silver_reads_only_the_latest_bronze_load():
    import retail_silver_clean as silver

    assert "max(_run_date)" in silver._latest("bronze.pos_sales")
    for name, (select_sql, *_rest) in silver.SOURCES.items():
        assert "_run_date = (SELECT max(_run_date)" in select_sql, name


def test_quarantine_serialisation_handles_decimals_and_dates():
    import retail_silver_clean as silver

    out = silver._json_safe({"p": decimal.Decimal("9.99"), "d": date(2026, 1, 2),
                             "t": datetime(2026, 1, 2, 3, 4), "n": [decimal.Decimal("1")]})
    assert out == {"p": 9.99, "d": "2026-01-02", "t": "2026-01-02T03:04:00", "n": [1.0]}
