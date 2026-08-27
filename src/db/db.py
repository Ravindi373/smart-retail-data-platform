"""
src/db/db.py

Shared Postgres connection helper and idempotent table creation for the
Bronze and Silver layers. Kept separate from the postgres-init SQL script
because that script only runs once, on first-ever volume creation — these
CREATE TABLE IF NOT EXISTS statements run at the start of every DAG run,
so the schema is guaranteed present regardless of when this code was
added or whether the Postgres volume was created before or after it.
"""

import os
import psycopg2


def get_conn():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="retaildb",
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


BRONZE_DDL = """
CREATE TABLE IF NOT EXISTS bronze.pos_sales (
    transaction_id  TEXT,
    store_id        TEXT,
    product_id      TEXT,
    customer_id     TEXT,
    quantity        INTEGER,
    unit_price      NUMERIC,
    raw_timestamp   TEXT,
    payment_method  TEXT,
    _source_file    TEXT,
    _ingested_at    TIMESTAMP,
    _run_date       DATE
);
CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id     TEXT,
    name            TEXT,
    email           TEXT,
    phone           TEXT,
    loyalty_tier    TEXT,
    created_at      TEXT,
    _source_file    TEXT,
    _ingested_at    TIMESTAMP,
    _run_date       DATE
);
CREATE TABLE IF NOT EXISTS bronze.products (
    product_id      TEXT,
    sku             TEXT,
    name            TEXT,
    category        TEXT,
    unit_cost       NUMERIC,
    unit_price      NUMERIC,
    _source_file    TEXT,
    _ingested_at    TIMESTAMP,
    _run_date       DATE
);
CREATE TABLE IF NOT EXISTS bronze.inventory_snapshots (
    snapshot_date     TEXT,
    product_id        TEXT,
    warehouse_id      TEXT,
    quantity_on_hand  NUMERIC,
    reorder_point     INTEGER,
    _source_file      TEXT,
    _ingested_at      TIMESTAMP,
    _run_date         DATE
);
CREATE TABLE IF NOT EXISTS bronze.ecommerce_orders (
    order_id        TEXT,
    customer_id     TEXT,
    items           JSONB,
    raw_timestamp   TEXT,
    channel         TEXT,
    status          TEXT,
    _source_file    TEXT,
    _ingested_at    TIMESTAMP,
    _run_date       DATE
);
CREATE TABLE IF NOT EXISTS bronze.supplier_deliveries (
    po_id           TEXT,
    supplier_id     TEXT,
    product_id      TEXT,
    ordered_qty     INTEGER,
    delivered_qty   INTEGER,
    ordered_date    TEXT,
    delivered_date  TEXT,
    _source_file    TEXT,
    _ingested_at    TIMESTAMP,
    _run_date       DATE
);
"""

SILVER_DDL = """
CREATE TABLE IF NOT EXISTS silver.pos_sales (
    transaction_id  TEXT PRIMARY KEY,
    store_id        TEXT,
    product_id      TEXT,
    customer_id     TEXT,
    quantity        INTEGER,
    unit_price      NUMERIC,
    txn_timestamp   TIMESTAMPTZ,
    payment_method  TEXT,
    _cleaned_at     TIMESTAMP
);
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id     TEXT PRIMARY KEY,
    name            TEXT,
    email_hash      TEXT,
    phone_hash      TEXT,
    loyalty_tier    TEXT,
    created_at      DATE,
    _cleaned_at     TIMESTAMP
);
CREATE TABLE IF NOT EXISTS silver.products (
    product_id      TEXT PRIMARY KEY,
    sku             TEXT,
    name            TEXT,
    category        TEXT,
    unit_cost       NUMERIC,
    unit_price      NUMERIC,
    _cleaned_at     TIMESTAMP
);
CREATE TABLE IF NOT EXISTS silver.inventory_snapshots (
    snapshot_id       TEXT PRIMARY KEY,
    snapshot_date     DATE,
    product_id        TEXT,
    warehouse_id      TEXT,
    quantity_on_hand  INTEGER,
    reorder_point     INTEGER,
    _cleaned_at       TIMESTAMP
);
CREATE TABLE IF NOT EXISTS silver.ecommerce_orders (
    order_id         TEXT PRIMARY KEY,
    customer_id      TEXT,
    items            JSONB,
    order_timestamp  TIMESTAMPTZ,
    channel          TEXT,
    status           TEXT,
    _cleaned_at      TIMESTAMP
);
CREATE TABLE IF NOT EXISTS silver.supplier_deliveries (
    po_id           TEXT PRIMARY KEY,
    supplier_id     TEXT,
    product_id      TEXT,
    ordered_qty     INTEGER,
    delivered_qty   INTEGER,
    ordered_date    DATE,
    delivered_date  DATE,
    _cleaned_at     TIMESTAMP
);
"""


def ensure_tables():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(BRONZE_DDL)
            cur.execute(SILVER_DDL)
        conn.commit()
    finally:
        conn.close()
