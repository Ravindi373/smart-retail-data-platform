"""
dags/retail_silver_clean.py

Silver pipeline: reads the latest Bronze load of every source, validates each
row against the rules in src/quality/rules.py, hashes PII, standardises
timestamps to UTC, resolves product identifiers to the product master, removes
duplicates by business key, and writes clean rows to the matching Silver table.

Any row that fails validation, or that repeats a business key already seen in
the same run, is NOT silently dropped - it is written to quality.quarantine
with a machine-readable failed_rule, per the project's "do not hide bad
records" rule. quality.run_summary records rows in / clean / quarantined per
source so the dashboard can show a pass rate.

Design decisions (all deliberate, all explained in the final report)
  * Full refresh: every run TRUNCATEs and rebuilds Silver. At this project's
    scale that is simpler and safer than incremental merge logic; a
    production system would use incremental/merge processing.
  * Latest load only: the sources are full extracts, so Silver is built from
    the most recent Bronze load (max(_run_date)) of each source. Older loads
    stay in Bronze for audit and replay. Without this, a second daily run
    would see every record twice and quarantine the copies as duplicates.
  * "Future-dated" means later than the source extract time (see
    rules.resolve_as_of), not later than today's wall clock, so the result is
    reproducible no matter when the pipeline runs.
  * Products are cleaned first because every other source resolves its
    product identifier (product_id or SKU) against silver.products.
"""

import decimal
import logging
import os
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from datetime import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import Json, RealDictCursor, execute_values

sys.path.insert(0, os.environ.get("RETAIL_SRC_DIR", "/opt/airflow/src"))
from db.db import ensure_tables, get_conn  # noqa: E402
from quality.rules import (  # noqa: E402
    build_product_lookup,
    clean_label,
    clean_text,
    hash_pii,
    parse_mixed_timestamp,
    resolve_as_of,
    resolve_order_items,
    resolve_product_id,
    validate_customer,
    validate_ecommerce_order,
    validate_inventory_snapshot,
    validate_pos_sale,
)

log = logging.getLogger(__name__)

SAMPLE_DIR = os.environ.get("RETAIL_SAMPLE_DIR", "/opt/airflow/data/sample")
MANIFEST_PATH = os.path.join(SAMPLE_DIR, "_manifest.json")


def _utcnow():
    """Naive UTC timestamp, matching the TIMESTAMP columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json_safe(value):
    """Recursively convert Decimal/date/datetime values into JSON-safe types
    before writing a quarantined row to quality.quarantine. psycopg2 returns
    NUMERIC columns as Decimal, which the standard JSON encoder cannot
    serialize directly."""
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


def _latest(table):
    """WHERE clause selecting only the most recent Bronze load of a table."""
    return f"_run_date = (SELECT max(_run_date) FROM {table})"


def _load_context(conn):
    """Reference data every row-level rule needs: the product lookup (from the
    already-clean silver.products) and the source extract time."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT product_id, sku FROM silver.products")
        products = build_product_lookup(cur.fetchall())
    as_of, origin = resolve_as_of(MANIFEST_PATH)
    if origin.startswith("fallback"):
        log.warning("no source extract time found - using current time as the "
                    "future-date reference; results will vary run to run")
    log.info("future-date reference (as_of) = %s [%s]; %d known product identifiers",
             as_of.isoformat(), origin, len(products))
    return {"as_of": as_of, "products": products}


# --------------------------------------------------------------------------
# Generic clean-and-quarantine runner
# --------------------------------------------------------------------------

def _clean_source(source_name, select_sql, row_fn, key_fn, silver_table, insert_cols,
                  needs_context=False, **_):
    """Read Bronze rows, apply row_fn, quarantine failures and duplicates, then
    rebuild the Silver table.

    row_fn(row, ctx) -> (failed_rule or None, values tuple or None)
    key_fn(row)      -> the business key used for duplicate detection
    """
    started = time.monotonic()
    conn = get_conn()
    try:
        ctx = _load_context(conn) if needs_context else {}
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(select_sql)
            rows = cur.fetchall()
        if not rows:
            log.warning("[%s] Bronze returned 0 rows - has retail_bronze_ingestion run?", source_name)

        clean, seen, reasons = [], set(), Counter()
        with conn.cursor() as cur:
            for row in rows:
                reason, values = row_fn(row, ctx)
                if reason is None:
                    key = key_fn(row)
                    if key in seen:
                        reason = "duplicate_business_key"
                    else:
                        seen.add(key)
                if reason:
                    _quarantine(cur, source_name, dict(row), reason)
                    reasons[reason] += 1
                    continue
                clean.append(values + (_utcnow(),))

            cur.execute(f"TRUNCATE {silver_table}")
            if clean:
                cols = ", ".join(list(insert_cols) + ["_cleaned_at"])
                execute_values(cur, f"INSERT INTO {silver_table} ({cols}) VALUES %s", clean)
            cur.execute(
                """INSERT INTO quality.run_summary
                       (source_name, rows_in, rows_clean, rows_quarantined, run_at)
                   VALUES (%s, %s, %s, %s, now())
                   ON CONFLICT (source_name) DO UPDATE SET
                       rows_in = EXCLUDED.rows_in,
                       rows_clean = EXCLUDED.rows_clean,
                       rows_quarantined = EXCLUDED.rows_quarantined,
                       run_at = EXCLUDED.run_at""",
                (source_name, len(rows), len(clean), len(rows) - len(clean)),
            )
        conn.commit()
        log.info(
            "[%s] rows_in=%d clean=%d quarantined=%d reasons=%s duration=%.1fs",
            source_name, len(rows), len(clean), len(rows) - len(clean),
            dict(reasons) or "{}", time.monotonic() - started,
        )
    except Exception:
        conn.rollback()
        log.exception("[%s] Silver cleaning failed; transaction rolled back", source_name)
        raise
    finally:
        conn.close()


# --------------------------------------------------------------------------
# Per-source row rules
# --------------------------------------------------------------------------

def _product_row(row, ctx):
    if not row["product_id"]:
        return "missing_product_id", None
    return None, (
        row["product_id"], clean_text(row["sku"]), clean_text(row["name"]),
        clean_text(row["category"]), row["unit_cost"], row["unit_price"],
    )


def _customer_row(row, ctx):
    reason = validate_customer(row)
    if reason:
        return reason, None
    try:
        created = datetime.strptime(row["created_at"], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        created = None
    return None, (
        row["customer_id"], clean_text(row["name"]), hash_pii(row["email"]),
        hash_pii(row["phone"]), clean_label(row["loyalty_tier"]), created,
    )


def _store_row(row, ctx):
    if not row["store_id"]:
        return "missing_store_id", None
    return None, (row["store_id"], clean_text(row["region"]), clean_label(row["channel"]))


def _pos_row(row, ctx):
    reason = validate_pos_sale(row, as_of=ctx["as_of"], product_lookup=ctx["products"])
    if reason:
        return reason, None
    return None, (
        row["transaction_id"], clean_text(row["store_id"]),
        resolve_product_id(row["product_id"], ctx["products"]),
        clean_text(row["customer_id"]), int(row["quantity"]), row["unit_price"],
        parse_mixed_timestamp(row["timestamp"]), clean_label(row["payment_method"]),
    )


def _ecommerce_row(row, ctx):
    reason = validate_ecommerce_order(row, as_of=ctx["as_of"], product_lookup=ctx["products"])
    if reason:
        return reason, None
    items, _ = resolve_order_items(row["items"], ctx["products"])
    return None, (
        row["order_id"], clean_text(row["customer_id"]), Json(items),
        parse_mixed_timestamp(row["order_timestamp"]),
        clean_label(row["channel"]), clean_label(row["status"]),
    )


def _inventory_row(row, ctx):
    reason = validate_inventory_snapshot(row, product_lookup=ctx["products"])
    if reason:
        return reason, None
    product_id = resolve_product_id(row["product_id"], ctx["products"])
    snap_date = datetime.strptime(str(row["snapshot_date"]), "%Y-%m-%d").date()
    reorder = int(row["reorder_point"]) if row["reorder_point"] is not None else None
    return None, (
        f"{product_id}_{row['warehouse_id']}_{snap_date.isoformat()}", snap_date,
        product_id, row["warehouse_id"], int(float(row["quantity_on_hand"])), reorder,
    )


def _supplier_row(row, ctx):
    if not row["po_id"]:
        return "missing_po_id", None
    return None, (
        row["po_id"], clean_text(row["supplier_id"]), row["product_id"],
        row["ordered_qty"], row["delivered_qty"], row["ordered_date"], row["delivered_date"],
    )


# name -> (bronze select, row_fn, key_fn, silver table, silver insert columns, needs_context)
SOURCES = {
    "products": (
        f"""SELECT product_id, sku, name, category, unit_cost, unit_price
            FROM bronze.products WHERE {_latest('bronze.products')}""",
        _product_row, lambda r: r["product_id"], "silver.products",
        ["product_id", "sku", "name", "category", "unit_cost", "unit_price"], False),
    "customers": (
        f"""SELECT customer_id, name, email, phone, loyalty_tier, created_at
            FROM bronze.customers WHERE {_latest('bronze.customers')}""",
        _customer_row, lambda r: r["customer_id"], "silver.customers",
        ["customer_id", "name", "email_hash", "phone_hash", "loyalty_tier", "created_at"], False),
    "stores": (
        f"SELECT store_id, region, channel FROM bronze.stores WHERE {_latest('bronze.stores')}",
        _store_row, lambda r: r["store_id"], "silver.stores",
        ["store_id", "region", "channel"], False),
    "pos_sales": (
        f"""SELECT transaction_id, store_id, product_id, customer_id, quantity, unit_price,
                   raw_timestamp AS timestamp, payment_method
            FROM bronze.pos_sales WHERE {_latest('bronze.pos_sales')}""",
        _pos_row, lambda r: r["transaction_id"], "silver.pos_sales",
        ["transaction_id", "store_id", "product_id", "customer_id", "quantity",
         "unit_price", "txn_timestamp", "payment_method"], True),
    "ecommerce_orders": (
        f"""SELECT order_id, customer_id, items, raw_timestamp AS order_timestamp, channel, status
            FROM bronze.ecommerce_orders WHERE {_latest('bronze.ecommerce_orders')}""",
        _ecommerce_row, lambda r: r["order_id"], "silver.ecommerce_orders",
        ["order_id", "customer_id", "items", "order_timestamp", "channel", "status"], True),
    "inventory_snapshots": (
        f"""SELECT snapshot_date, product_id, warehouse_id, quantity_on_hand, reorder_point
            FROM bronze.inventory_snapshots WHERE {_latest('bronze.inventory_snapshots')}""",
        _inventory_row, lambda r: (r["snapshot_date"], r["product_id"], r["warehouse_id"]),
        "silver.inventory_snapshots",
        ["snapshot_id", "snapshot_date", "product_id", "warehouse_id",
         "quantity_on_hand", "reorder_point"], True),
    "supplier_deliveries": (
        f"""SELECT po_id, supplier_id, product_id, ordered_qty, delivered_qty, ordered_date, delivered_date
            FROM bronze.supplier_deliveries WHERE {_latest('bronze.supplier_deliveries')}""",
        _supplier_row, lambda r: r["po_id"], "silver.supplier_deliveries",
        ["po_id", "supplier_id", "product_id", "ordered_qty", "delivered_qty",
         "ordered_date", "delivered_date"], False),
}


def _clean_named_source(source_name, **_):
    select_sql, row_fn, key_fn, silver_table, insert_cols, needs_context = SOURCES[source_name]
    _clean_source(source_name, select_sql, row_fn, key_fn, silver_table, insert_cols,
                  needs_context=needs_context)


# --------------------------------------------------------------------------
# Housekeeping tasks
# --------------------------------------------------------------------------

def _ensure_tables(**_):
    ensure_tables()


def _clear_quality_tables(**_):
    """Truncate quality.quarantine and quality.run_summary at the start of every
    run so they reflect only the current run's findings, matching the
    full-refresh semantics used for silver.* tables. Without this, quarantine
    grows unbounded across runs, producing misleading totals (e.g. more
    quarantined rows than a source table even has)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE quality.quarantine, quality.run_summary")
        conn.commit()
    finally:
        conn.close()


def _log_quality_report(**_):
    """Write the run's quality summary to the task log - the pass rate per
    source plus the top failure reasons - so it is visible in Airflow."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT source_name, rows_in, rows_clean, rows_quarantined
                           FROM quality.run_summary ORDER BY source_name""")
            summary = cur.fetchall()
            cur.execute("""SELECT source_name, failed_rule, count(*) FROM quality.quarantine
                           GROUP BY 1, 2 ORDER BY 1, 3 DESC""")
            failures = cur.fetchall()
    finally:
        conn.close()
    log.info("=== SILVER DATA QUALITY REPORT ===")
    log.info("%-22s %8s %8s %12s %10s", "source", "rows_in", "clean", "quarantined", "pass_rate")
    for name, rows_in, clean, quarantined in summary:
        rate = (100.0 * clean / rows_in) if rows_in else 0.0
        log.info("%-22s %8d %8d %12d %9.1f%%", name, rows_in, clean, quarantined, rate)
    for name, rule, count in failures:
        log.info("  quarantined  %-22s %-28s %6d", name, rule, count)


default_args = {
    "owner": "retaillake",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retail_silver_clean",
    description="Clean Bronze tables into Silver, quarantining invalid/duplicate rows",
    default_args=default_args,
    schedule=None,  # triggered by retail_pipeline (or manually),
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retaillake", "silver", "quality"],
) as dag:

    ensure_tables_task = PythonOperator(task_id="ensure_bronze_silver_tables", python_callable=_ensure_tables)
    clear_task = PythonOperator(task_id="clear_quality_tables", python_callable=_clear_quality_tables)

    tasks = {
        name: PythonOperator(
            task_id=f"clean_{name}",
            python_callable=_clean_named_source,
            op_kwargs={"source_name": name},
        )
        for name in SOURCES
    }
    report_task = PythonOperator(task_id="log_quality_report", python_callable=_log_quality_report)

    ensure_tables_task >> clear_task >> tasks["products"]
    # products first: every other product-bearing source resolves against it
    tasks["products"] >> [tasks["pos_sales"], tasks["ecommerce_orders"], tasks["inventory_snapshots"]]
    clear_task >> [tasks["customers"], tasks["stores"], tasks["supplier_deliveries"]]
    list(tasks.values()) >> report_task
