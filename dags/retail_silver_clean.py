"""
dags/retail_silver_clean.py

Week 4 pipeline: reads each Bronze table in full, validates every row
against the rules in src/quality/rules.py, hashes PII fields, standardises
timestamps to UTC, and writes the result to the matching Silver table.

Any row that fails validation, or that repeats a business key already
seen earlier in the same run, is NOT silently dropped — it is written to
quality.quarantine with a human-readable failed_rule, per the project's
"do not hide bad records" quality rule.

Approach: full-refresh. Each run reads ALL of Bronze (not just the
current logical date) and rebuilds Silver from scratch. This is a
deliberate simplification appropriate to this project's local, batch-
oriented scale — documented here rather than silently assumed, since a
production system at larger scale would use incremental/merge logic
instead of a full rebuild on every run.
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import execute_values, Json, RealDictCursor

sys.path.insert(0, "/opt/airflow/src")
from db.db import get_conn, ensure_tables                          # noqa: E402
from quality.rules import (                                        # noqa: E402
    validate_pos_sale, validate_customer, validate_ecommerce_order,
    validate_inventory_snapshot, parse_mixed_timestamp, hash_pii,
)


import decimal
from datetime import date, datetime as dt


def _json_safe(value):
    """Recursively convert Decimal/date/datetime values into JSON-safe
    types before writing a quarantined row to quality.quarantine.
    psycopg2 returns NUMERIC columns as Decimal, which the standard JSON
    encoder cannot serialize directly."""
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (date, dt)):
        return value.isoformat()
    return value


def _quarantine(cur, source_name, record, failed_rule):
    cur.execute(
        """INSERT INTO quality.quarantine (source_name, record_json, failed_rule)
           VALUES (%s, %s, %s)""",
        (source_name, Json(_json_safe(record)), failed_rule),
    )


def _clean_pos_sales(**_):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT transaction_id, store_id, product_id, customer_id,
                                  quantity, unit_price, raw_timestamp AS timestamp,
                                  payment_method FROM bronze.pos_sales""")
            rows = cur.fetchall()

        clean, seen_keys = [], set()
        with conn.cursor() as cur:
            for row in rows:
                reason = validate_pos_sale(row)
                if reason:
                    _quarantine(cur, "pos_sales", dict(row), reason)
                    continue
                if row["transaction_id"] in seen_keys:
                    _quarantine(cur, "pos_sales", dict(row), "duplicate_business_key")
                    continue
                seen_keys.add(row["transaction_id"])
                ts = parse_mixed_timestamp(row["timestamp"])
                clean.append((
                    row["transaction_id"], row["store_id"], row["product_id"],
                    row["customer_id"], int(row["quantity"]), row["unit_price"],
                    ts, row["payment_method"], datetime.utcnow(),
                ))
            cur.execute("TRUNCATE silver.pos_sales")
            execute_values(
                cur,
                """INSERT INTO silver.pos_sales
                   (transaction_id, store_id, product_id, customer_id, quantity,
                    unit_price, txn_timestamp, payment_method, _cleaned_at)
                   VALUES %s""",
                clean,
            )
        conn.commit()
        print(f"[pos_sales] {len(clean)} clean rows, {len(rows) - len(clean)} quarantined")
    finally:
        conn.close()


def _clean_customers(**_):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT customer_id, name, email, phone, loyalty_tier,
                                  created_at FROM bronze.customers""")
            rows = cur.fetchall()

        clean, seen_keys = [], set()
        with conn.cursor() as cur:
            for row in rows:
                reason = validate_customer(row)
                if reason:
                    _quarantine(cur, "customers", dict(row), reason)
                    continue
                if row["customer_id"] in seen_keys:
                    _quarantine(cur, "customers", dict(row), "duplicate_business_key")
                    continue
                seen_keys.add(row["customer_id"])
                created = None
                try:
                    created = datetime.strptime(row["created_at"], "%Y-%m-%d").date()
                except (ValueError, TypeError):
                    pass
                clean.append((
                    row["customer_id"], row["name"], hash_pii(row["email"]),
                    hash_pii(row["phone"]), row["loyalty_tier"], created,
                    datetime.utcnow(),
                ))
            cur.execute("TRUNCATE silver.customers")
            execute_values(
                cur,
                """INSERT INTO silver.customers
                   (customer_id, name, email_hash, phone_hash, loyalty_tier,
                    created_at, _cleaned_at)
                   VALUES %s""",
                clean,
            )
        conn.commit()
        print(f"[customers] {len(clean)} clean rows, {len(rows) - len(clean)} quarantined")
    finally:
        conn.close()


def _clean_ecommerce_orders(**_):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT order_id, customer_id, items,
                                  raw_timestamp AS order_timestamp, channel, status
                           FROM bronze.ecommerce_orders""")
            rows = cur.fetchall()

        clean, seen_keys = [], set()
        with conn.cursor() as cur:
            for row in rows:
                reason = validate_ecommerce_order(row)
                if reason:
                    _quarantine(cur, "ecommerce_orders", dict(row), reason)
                    continue
                if row["order_id"] in seen_keys:
                    _quarantine(cur, "ecommerce_orders", dict(row), "duplicate_business_key")
                    continue
                seen_keys.add(row["order_id"])
                ts = parse_mixed_timestamp(row["order_timestamp"])
                clean.append((
                    row["order_id"], row["customer_id"], Json(row["items"]),
                    ts, row["channel"], row["status"], datetime.utcnow(),
                ))
            cur.execute("TRUNCATE silver.ecommerce_orders")
            execute_values(
                cur,
                """INSERT INTO silver.ecommerce_orders
                   (order_id, customer_id, items, order_timestamp, channel,
                    status, _cleaned_at)
                   VALUES %s""",
                clean,
            )
        conn.commit()
        print(f"[ecommerce_orders] {len(clean)} clean rows, {len(rows) - len(clean)} quarantined")
    finally:
        conn.close()


def _clean_inventory_snapshots(**_):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT snapshot_date, product_id, warehouse_id,
                                  quantity_on_hand, reorder_point
                           FROM bronze.inventory_snapshots""")
            rows = cur.fetchall()

        clean, seen_keys = [], set()
        with conn.cursor() as cur:
            for row in rows:
                reason = validate_inventory_snapshot(row)
                if reason:
                    _quarantine(cur, "inventory_snapshots", dict(row), reason)
                    continue
                key = (row["snapshot_date"], row["product_id"], row["warehouse_id"])
                if key in seen_keys:
                    _quarantine(cur, "inventory_snapshots", dict(row), "duplicate_business_key")
                    continue
                seen_keys.add(key)
                snap_date = None
                try:
                    snap_date = datetime.strptime(row["snapshot_date"], "%Y-%m-%d").date()
                except (ValueError, TypeError):
                    pass
                snapshot_id = f"{row['product_id']}_{row['warehouse_id']}_{row['snapshot_date']}"
                clean.append((
                    snapshot_id, snap_date, row["product_id"], row["warehouse_id"],
                    int(float(row["quantity_on_hand"])),
                    int(row["reorder_point"]) if row["reorder_point"] is not None else None,
                    datetime.utcnow(),
                ))
            cur.execute("TRUNCATE silver.inventory_snapshots")
            execute_values(
                cur,
                """INSERT INTO silver.inventory_snapshots
                   (snapshot_id, snapshot_date, product_id, warehouse_id,
                    quantity_on_hand, reorder_point, _cleaned_at)
                   VALUES %s""",
                clean,
            )
        conn.commit()
        print(f"[inventory_snapshots] {len(clean)} clean rows, {len(rows) - len(clean)} quarantined")
    finally:
        conn.close()


def _clean_passthrough(source_name, bronze_table, silver_table, key_col, select_cols, insert_cols, **_):
    """Shared logic for products and supplier_deliveries: no business
    validation rules defined for these in this iteration, but still
    deduplicated by business key so 'duplicate business keys are
    handled' applies uniformly across all six sources."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT {', '.join(select_cols)} FROM {bronze_table}")
            rows = cur.fetchall()

        clean, seen_keys = [], set()
        with conn.cursor() as cur:
            for row in rows:
                if row[key_col] is None:
                    _quarantine(cur, source_name, dict(row), f"missing_{key_col}")
                    continue
                if row[key_col] in seen_keys:
                    _quarantine(cur, source_name, dict(row), "duplicate_business_key")
                    continue
                seen_keys.add(row[key_col])
                clean.append(tuple(row[c] for c in select_cols) + (datetime.utcnow(),))
            cur.execute(f"TRUNCATE {silver_table}")
            col_list = ", ".join(insert_cols + ["_cleaned_at"])
            execute_values(cur, f"INSERT INTO {silver_table} ({col_list}) VALUES %s", clean)
        conn.commit()
        print(f"[{source_name}] {len(clean)} clean rows, {len(rows) - len(clean)} quarantined")
    finally:
        conn.close()


def _ensure_tables(**_):
    ensure_tables()


default_args = {
    "owner": "retaillake",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retail_silver_clean",
    description="Clean Bronze tables into Silver, quarantining invalid/duplicate rows",
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retaillake", "silver", "quality"],
) as dag:

    ensure_tables_task = PythonOperator(
        task_id="ensure_bronze_silver_tables",
        python_callable=_ensure_tables,
    )

    clean_pos_sales_task = PythonOperator(
        task_id="clean_pos_sales", python_callable=_clean_pos_sales
    )
    clean_customers_task = PythonOperator(
        task_id="clean_customers", python_callable=_clean_customers
    )
    clean_ecommerce_task = PythonOperator(
        task_id="clean_ecommerce_orders", python_callable=_clean_ecommerce_orders
    )
    clean_inventory_task = PythonOperator(
        task_id="clean_inventory_snapshots", python_callable=_clean_inventory_snapshots
    )
    clean_products_task = PythonOperator(
        task_id="clean_products",
        python_callable=_clean_passthrough,
        op_kwargs=dict(
            source_name="products", bronze_table="bronze.products",
            silver_table="silver.products", key_col="product_id",
            select_cols=["product_id", "sku", "name", "category", "unit_cost", "unit_price"],
            insert_cols=["product_id", "sku", "name", "category", "unit_cost", "unit_price"],
        ),
    )
    clean_suppliers_task = PythonOperator(
        task_id="clean_supplier_deliveries",
        python_callable=_clean_passthrough,
        op_kwargs=dict(
            source_name="supplier_deliveries", bronze_table="bronze.supplier_deliveries",
            silver_table="silver.supplier_deliveries", key_col="po_id",
            select_cols=["po_id", "supplier_id", "product_id", "ordered_qty",
                        "delivered_qty", "ordered_date", "delivered_date"],
            insert_cols=["po_id", "supplier_id", "product_id", "ordered_qty",
                        "delivered_qty", "ordered_date", "delivered_date"],
        ),
    )

    ensure_tables_task >> [
        clean_pos_sales_task, clean_customers_task, clean_ecommerce_task,
        clean_inventory_task, clean_products_task, clean_suppliers_task,
    ]
