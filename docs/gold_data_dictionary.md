# RetailLake — Gold Layer Data Dictionary

This document is the single reference for what every Gold table *is*,
not just what columns it has. Column-level descriptions and dbt tests
also live in `dbt_smart_retail/models/gold/_schema.yml` and
`_schema_dashboard.yml`, and are viewable interactively via
`dbt docs generate` — this document exists because "grain and business
meaning" deserves to be read as prose, not discovered by scrolling
through YAML.

For each table: **grain** (what one row represents), **purpose** (what
business question it answers), and **key fields** (the ones that matter
for interpretation, not an exhaustive column list).

---

## Dimensions

### `dim_customer`
- **Grain:** one row per customer.
- **Purpose:** answers "who is this customer" for any sales analysis —
  loyalty tier segmentation, customer-level reporting. PII (email,
  phone) is pre-hashed in Silver; this table never carries raw PII.
- **Key fields:** `customer_key` (business key, = Silver's
  `customer_id`), `loyalty_tier` (bronze/silver/gold/platinum/blank —
  blank is a legitimate "no enrollment" value, not missing data),
  `email_hash`/`phone_hash` (one-way hashed, not reversible).
- **Row count (verified):** 973 — customers who passed Silver validation.
  37 source customers were excluded (see Data Quality Report).

### `dim_product`
- **Grain:** one row per product. Pure reference dimension — this data
  doesn't change based on transactions.
- **Purpose:** answers "what is this product" — name, category, cost,
  and price for any sales or inventory analysis.
- **Key fields:** `product_key` (= `product_id`), `category` (6 values,
  used as a required dashboard filter), `unit_cost` vs `unit_price`
  (margin can be derived from these two).
- **Row count (verified):** 300.

### `dim_store`
- **Grain:** one row per physical store/warehouse location.
- **Purpose:** answers "where did this happen" for in-store sales and
  inventory snapshots.
- **Key fields:** `store_key` (= `store_id`/`warehouse_id` — the same
  identifier space is reused for both roles in the source data).
- **Known limitation:** `region` is always NULL. The mock data generator
  assigns a region internally when creating stores, but never writes it
  into any of the six source files, so it never reaches Bronze. A real
  fix requires a dedicated `stores.csv` source — not fabricated here.
- **Row count (verified):** 25 — matches the generator's actual store
  count exactly, independently confirming the join logic in this model
  is correct (not a coincidence — see Week 5 sync notes).

### `dim_date`
- **Grain:** one row per calendar date, 2020-01-01 to 2030-12-31.
- **Purpose:** the standard date dimension — enables year/month/quarter/
  weekend rollups without repeating date math in every query.
- **Key fields:** `date_key` (join key, a plain date), `is_weekend`
  (derived from ISO day-of-week).
- **Known limitation:** the range is static, not computed from the
  actual data. Fine at this project's scale; a production warehouse
  would compute the range dynamically instead.
- **Row count:** 4,018.

---

## Facts

### `fact_sales_transaction`
- **Grain:** one row per sales line item — one product, within one
  transaction (in-store) or one order (online). **This is not one row
  per transaction/order** — an order with 3 items produces 3 rows.
- **Purpose:** the core sales fact. Answers "how much did we sell,
  where, when, and through which channel" — feeds the Daily Sales
  Trend, Revenue by Channel, and Top Products dashboard charts.
- **How it's built:** a UNION of two genuinely different shapes —
  `silver.pos_sales` (already at line-item grain) and
  `silver.ecommerce_orders.items` (a JSON array per order, unnested via
  `jsonb_array_elements` so each array element becomes its own row).
  This union is the reason `fact_sales_transaction` exists as a
  materialized table rather than being computed ad hoc per chart — the
  unnesting logic is non-trivial and should live in one place.
- **Key fields:** `transaction_line_id` (composite for online rows:
  `order_id` + item index, since `order_id` alone repeats across an
  order's line items), `net_sales` = `quantity * unit_price`, `channel`
  (`in_store`/`online`/`mobile_app`).
- **Important nuance:** `store_key` is legitimately NULL for online
  rows — an online sale has no physical store. This is not missing
  data, and the dbt relationships test on this column only evaluates
  non-null values for exactly this reason.
- **Row count (verified):** 12,115 (4,792 in-store + ~7,323 unnested
  online line items).

### `fact_inventory_snapshot`
- **Grain:** one row per product, per store/warehouse, per snapshot
  date — a point-in-time stock count, not a running total.
- **Purpose:** answers "are we at risk of stocking out" — feeds the
  Stockout/Low-Stock dashboard table.
- **Key fields:** `snapshot_id` (synthetic: `product_key` +
  `store_key` + `date_key`), `is_stockout_risk` — **derived**, not
  sourced directly: `true` when `quantity_on_hand <= reorder_point`.
  This is the single most business-relevant field in the table and the
  reason this fact exists as a materialized transformation rather than
  a passthrough of Silver.
- **Row count (verified):** 493.

---

## Reporting views (not part of the star schema itself)

These two views exist purely to make Superset's job easier — they are
convenience joins over the facts and dimensions above, not new sources
of truth. If a number here disagrees with a chart built directly on the
star schema, the star schema is right and the join in these views has a
bug — check `sales_flat.sql`/`inventory_flat.sql` first.

### `sales_flat`
- **Grain:** matches `fact_sales_transaction` exactly (same row count,
  12,115) — this is a join, not an aggregation.
- **Purpose:** gives Superset every dashboard filter column (`channel`,
  `category`, `store_id`, `region`, `loyalty_tier`, `date_key`) in one
  place, so no chart needs to hand-join four tables just to filter by
  loyalty tier.

### `inventory_flat`
- **Grain:** matches `fact_inventory_snapshot` exactly (493 rows).
- **Purpose:** same rationale as `sales_flat`, for the stockout chart.

### `quality_summary`
- **Grain:** one row per (`source_name`, `failed_rule`) combination —
  an aggregate, not a passthrough.
- **Purpose:** answers "is the pipeline healthy" for the Data Reviewer
  persona — powers the Data Quality Summary dashboard tile. Built
  directly from `quality.quarantine`, which `retail_silver_clean`
  truncates and repopulates on every run (see the Pipeline Reliability
  doc for why that matters).

---

## How the star schema answers the brief's three personas

| Persona | Question | Table(s) |
|---|---|---|
| Business Manager | How much did we sell, by channel/product/day? | `fact_sales_transaction` + `dim_product`, `dim_date` |
| Inventory Planner | What's at risk of stocking out? | `fact_inventory_snapshot` (`is_stockout_risk`) |
| Data Reviewer | Is the pipeline trustworthy right now? | `quality_summary` |
