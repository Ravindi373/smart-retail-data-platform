"""
dags/retail_bronze_ingestion.py

Week 3 pipeline: lands each of the five source files (plus the products
reference file) into MinIO under a date-partitioned raw/ path, then
parses each into a typed Bronze Postgres table with ingestion metadata,
and records one row per source in bronze.ingestion_audit_log.

Idempotency: re-running this DAG for the same logical date does not
create duplicate raw objects (storage.land_raw_file skips identical
content) and does not duplicate Bronze rows (existing rows for the same
_run_date are deleted before the fresh batch is inserted).

Source data: for this local student project there is no live upstream
system to poll, so this DAG reads the generated mock files directly from
/opt/airflow/data/sample (mounted from the repo's data/sample/ folder).
In a production setting, the land_raw_file step would instead pull from
each system's real API, export, or database connection.
"""

import csv
import json
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from psycopg2.extras import execute_values, Json

sys.path.insert(0, "/opt/airflow/src")
from db.db import get_conn, ensure_tables          # noqa: E402
from connectors.storage import land_raw_file        # noqa: E402

SAMPLE_DIR = "/opt/airflow/data/sample"

# source_name, filename, bronze table, CSV column names, DB column names
# (CSV headers and DB columns match 1:1 except pos_sales, whose CSV
# "timestamp" column is stored as "raw_timestamp" in Bronze to match the
# Silver DAG's SELECT ... AS timestamp aliasing)
# NOTE: supplier_deliveries.json is JSON, not CSV — it has its own
# dedicated handler (_land_and_parse_supplier_deliveries) below, same
# pattern as ecommerce_orders, and is NOT listed here.
SOURCES = [
    ("pos_sales", "pos_sales.csv", "bronze.pos_sales",
     ["transaction_id", "store_id", "product_id", "customer_id",
      "quantity", "unit_price", "timestamp", "payment_method"],
     ["transaction_id", "store_id", "product_id", "customer_id",
      "quantity", "unit_price", "raw_timestamp", "payment_method"]),
    ("customers", "customers.csv", "bronze.customers",
     ["customer_id", "name", "email", "phone", "loyalty_tier", "created_at"],
     ["customer_id", "name", "email", "phone", "loyalty_tier", "created_at"]),
    ("products", "products.csv", "bronze.products",
     ["product_id", "sku", "name", "category", "unit_cost", "unit_price"],
     ["product_id", "sku", "name", "category", "unit_cost", "unit_price"]),
    ("inventory_snapshots", "inventory_snapshots.csv", "bronze.inventory_snapshots",
     ["snapshot_date", "product_id", "warehouse_id", "quantity_on_hand", "reorder_point"],
     ["snapshot_date", "product_id", "warehouse_id", "quantity_on_hand", "reorder_point"]),
]

NUMERIC_COLUMNS = {"quantity", "unit_price", "unit_cost", "quantity_on_hand",
                   "reorder_point", "ordered_qty", "delivered_qty"}


def _coerce(col, value):
    if col in NUMERIC_COLUMNS:
        if value is None or str(value).strip() == "":
            return None
        try:
            return float(value)
        except ValueError:
            return None
    return value


def _load_csv_rows(path, columns):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [tuple(_coerce(c, r.get(c)) for c in columns) for r in reader]


def _land_and_parse_csv(source_name, filename, table, columns, db_columns, run_date, **_):
    run_date = datetime.strptime(run_date, "%Y-%m-%d").date()
    local_path = os.path.join(SAMPLE_DIR, filename)
    object_key = land_raw_file(local_path, source_name, run_date)

    rows = _load_csv_rows(local_path, columns)
    now = datetime.utcnow()
    full_rows = [r + (filename, now, run_date) for r in rows]
    full_columns = db_columns + ["_source_file", "_ingested_at", "_run_date"]

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table} WHERE _run_date = %s", (run_date,))
            col_list = ", ".join(full_columns)
            execute_values(
                cur, f"INSERT INTO {table} ({col_list}) VALUES %s", full_rows
            )
            cur.execute(
                """INSERT INTO bronze.ingestion_audit_log
                   (source_name, file_path, row_count, run_date)
                   VALUES (%s, %s, %s, %s)""",
                (source_name, object_key, len(rows), run_date),
            )
        conn.commit()
    finally:
        conn.close()

    print(f"[{source_name}] landed at {object_key}, {len(rows)} rows -> {table}")


def _land_and_parse_ecommerce(run_date, **_):
    run_date = datetime.strptime(run_date, "%Y-%m-%d").date()
    filename = "ecommerce_orders.json"
    local_path = os.path.join(SAMPLE_DIR, filename)
    object_key = land_raw_file(local_path, "ecommerce_orders", run_date)

    with open(local_path, encoding="utf-8") as f:
        orders = json.load(f)

    now = datetime.utcnow()
    full_rows = [
        (
            o.get("order_id"),
            o.get("customer_id"),
            Json(o.get("items")),
            o.get("order_timestamp"),
            o.get("channel"),
            o.get("status"),
            filename,
            now,
            run_date,
        )
        for o in orders
    ]

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM bronze.ecommerce_orders WHERE _run_date = %s", (run_date,)
            )
            execute_values(
                cur,
                """INSERT INTO bronze.ecommerce_orders
                   (order_id, customer_id, items, raw_timestamp, channel, status,
                    _source_file, _ingested_at, _run_date)
                   VALUES %s""",
                full_rows,
            )
            cur.execute(
                """INSERT INTO bronze.ingestion_audit_log
                   (source_name, file_path, row_count, run_date)
                   VALUES (%s, %s, %s, %s)""",
                ("ecommerce_orders", object_key, len(orders), run_date),
            )
        conn.commit()
    finally:
        conn.close()

    print(f"[ecommerce_orders] landed at {object_key}, {len(orders)} rows -> bronze.ecommerce_orders")


def _land_and_parse_supplier_deliveries(run_date, **_):
    run_date = datetime.strptime(run_date, "%Y-%m-%d").date()
    filename = "supplier_deliveries.json"
    local_path = os.path.join(SAMPLE_DIR, filename)
    object_key = land_raw_file(local_path, "supplier_deliveries", run_date)

    with open(local_path, encoding="utf-8") as f:
        deliveries = json.load(f)

    now = datetime.utcnow()
    full_rows = [
        (
            d.get("po_id"),
            d.get("supplier_id"),
            d.get("product_id"),
            d.get("ordered_qty"),
            d.get("delivered_qty"),
            d.get("ordered_date"),
            d.get("delivered_date"),
            filename,
            now,
            run_date,
        )
        for d in deliveries
    ]

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM bronze.supplier_deliveries WHERE _run_date = %s", (run_date,)
            )
            execute_values(
                cur,
                """INSERT INTO bronze.supplier_deliveries
                   (po_id, supplier_id, product_id, ordered_qty, delivered_qty,
                    ordered_date, delivered_date, _source_file, _ingested_at, _run_date)
                   VALUES %s""",
                full_rows,
            )
            cur.execute(
                """INSERT INTO bronze.ingestion_audit_log
                   (source_name, file_path, row_count, run_date)
                   VALUES (%s, %s, %s, %s)""",
                ("supplier_deliveries", object_key, len(deliveries), run_date),
            )
        conn.commit()
    finally:
        conn.close()

    print(f"[supplier_deliveries] landed at {object_key}, {len(deliveries)} rows -> bronze.supplier_deliveries")


def _ensure_tables(**_):
    ensure_tables()


default_args = {
    "owner": "retaillake",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retail_bronze_ingestion",
    description="Land raw retail source files into MinIO and parse into Bronze tables",
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retaillake", "bronze", "ingestion"],
) as dag:

    ensure_tables_task = PythonOperator(
        task_id="ensure_bronze_silver_tables",
        python_callable=_ensure_tables,
    )

    with TaskGroup("land_and_parse") as land_and_parse:
        for source_name, filename, table, columns, db_columns in SOURCES:
            PythonOperator(
                task_id=f"land_and_parse_{source_name}",
                python_callable=_land_and_parse_csv,
                op_kwargs={
                    "source_name": source_name,
                    "filename": filename,
                    "table": table,
                    "columns": columns,
                    "db_columns": db_columns,
                    "run_date": "{{ ds }}",
                },
            )

        PythonOperator(
            task_id="land_and_parse_ecommerce_orders",
            python_callable=_land_and_parse_ecommerce,
            op_kwargs={"run_date": "{{ ds }}"},
        )

        PythonOperator(
            task_id="land_and_parse_supplier_deliveries",
            python_callable=_land_and_parse_supplier_deliveries,
            op_kwargs={"run_date": "{{ ds }}"},
        )

    ensure_tables_task >> land_and_parse