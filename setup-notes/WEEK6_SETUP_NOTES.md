# Dashboard setup notes: Superset (brief Week 6 task / Week 8 deliverable row)

The project brief lists dashboard-building under Week 6's task list, but
also under the "Dashboard and report" row of the final Deliverables
table (due Week 8). This covers the actual dashboard build — do it now
so it's not a Week 8 crunch item, and revisit polish/screenshots before
final submission.

## What this adds

- `dbt_smart_retail/models/gold/sales_flat.sql` — a denormalized,
  dashboard-ready view joining fact_sales_transaction to every dimension.
  This is the one dataset Superset needs for 3 of the 5 required charts,
  and carries every required filter column.
- `dbt_smart_retail/models/gold/inventory_flat.sql` — same idea, for the
  stockout/low-stock chart.
- `dbt_smart_retail/models/gold/_schema_dashboard.yml` — descriptions +
  basic tests for the two new views.
- A new `superset` service for `docker/docker-compose.yml`.

## Verified before delivery

Both new views were tested directly against real Gold data (same
Postgres instance used to verify Week 5's models) before being handed
to you:

| View | Rows | Filter coverage confirmed |
|---|---|---|
| sales_flat | 12,115 | 3 channels, 6 categories, 25 stores, 5 loyalty tiers, full date range |
| inventory_flat | 493 | is_stockout_risk correctly flags low-stock rows |

Row counts match `fact_sales_transaction` and `fact_inventory_snapshot`
exactly — these views add columns, they don't change grain or row count.

**Known data note:** a handful of `sales_flat.loyalty_tier` values are
blank. This isn't a bug — the mock data generator assigns `None` as one
of five possible loyalty tier values, matching how a real system would
have customers with no loyalty program enrollment. Worth a one-line
mention in your final report rather than something to "fix."

## Docker Compose changes needed

1. Add the `superset` service from `docker/superset-service-addition.yml`
   into your real `docker/docker-compose.yml`, as a sibling of your other
   services.
2. Add `superset_home:` to the `volumes:` section at the bottom of the
   file.
3. Add one new line to `.env` (not `.env.example` — this is a real
   secret, generate your own):
   ```
   SUPERSET_SECRET_KEY=<run: python -c "import secrets; print(secrets.token_hex(32))">
   ```
   Add a placeholder line to `.env.example` too:
   ```
   SUPERSET_SECRET_KEY=replace-with-generated-secret-key
   ```

## How to run

1. Copy `dbt_smart_retail/models/gold/sales_flat.sql`,
   `inventory_flat.sql`, and `_schema_dashboard.yml` into your repo.
2. Apply the Docker Compose changes above.
3. Start the stack:
   ```
   docker compose --env-file ../.env up -d
   ```
   Superset's image is large (~2GB+) — first pull will take a while.
4. Build the two new views:
   ```
   docker exec -it retaillake-dbt dbt run --select sales_flat inventory_flat
   ```
5. Initialize Superset (one-time setup — run these three commands in order):
   ```
   docker exec -it retaillake-superset superset db upgrade
   docker exec -it retaillake-superset superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@example.com --password <choose-a-password>
   docker exec -it retaillake-superset superset init
   ```
6. Open `http://localhost:8088` and log in with the admin credentials
   from step 5.

## Connect Superset to Postgres

1. Settings (top right) → **Database Connections** → **+ Database**.
2. Choose **PostgreSQL**.
3. SQLAlchemy URI (use your real `.env` Postgres credentials):
   ```
   postgresql://retaillake:<POSTGRES_PASSWORD>@postgres:5432/retaildb
   ```
4. Test the connection, then **Connect**.

## Create the datasets

**Data** → **Datasets** → **+ Dataset**. Create three, all from schema
`gold`:
- `sales_flat`
- `inventory_flat`
- `quality_summary`

## Build the 5 required charts

**Charts** → **+ Chart** for each:

1. **Daily sales trend**
   Dataset: `sales_flat`. Chart type: **Line Chart**.
   X-axis: `date_key` (time column, grain = Day).
   Metric: `SUM(net_sales)`.

2. **Revenue by channel**
   Dataset: `sales_flat`. Chart type: **Bar Chart**.
   Dimension: `channel`. Metric: `SUM(net_sales)`.

3. **Top products by net sales**
   Dataset: `sales_flat`. Chart type: **Bar Chart** (or Table).
   Dimension: `product_name`. Metric: `SUM(net_sales)`.
   Sort descending, row limit 10.

4. **Stockout / low-stock table**
   Dataset: `inventory_flat`. Chart type: **Table**.
   Columns: `store_id`, `product_name`, `category`, `quantity_on_hand`, `reorder_point`.
   Filter: `is_stockout_risk = true`.

5. **Data quality summary**
   Dataset: `quality_summary`. Chart type: **Table** (or Bar).
   Columns: `source_name`, `failed_rule`, `quarantined_count`.

## Assemble the dashboard

1. **Dashboards** → **+ Dashboard**. Name it e.g. "RetailLake — Business Overview".
2. Drag all 5 charts onto the canvas.
3. Add native dashboard filters (top-left funnel icon → **+ Add/Edit Filters**),
   one per brief requirement:
   - **Date range** — column `date_key`, applies to `sales_flat` + `inventory_flat`
   - **Sales channel** — column `channel`, applies to `sales_flat`
   - **Store or region** — column `store_id` (or `region`), applies to `sales_flat` + `inventory_flat`
   - **Product category** — column `category`, applies to `sales_flat` + `inventory_flat`
   - **Customer loyalty tier** — column `loyalty_tier`, applies to `sales_flat`
4. Save the dashboard.

## Evidence to capture for your mentor sync

- Screenshot of the full dashboard with all 5 charts visible
- Screenshot with a filter applied (e.g. channel = online), showing
  charts update together
- Screenshot of the stockout table specifically
- The dataset list showing `sales_flat`, `inventory_flat`,
  `quality_summary`
- A one-line note per the brief's demo checklist: *"Can a manager
  understand sales, stock and data health in three minutes?"* — answer
  this yourself before the mentor sync, since it's the brief's own
  success criterion for this deliverable.

## Known limitation to mention in your final report

`sales_flat` and `inventory_flat` are materialized as **views**, not
tables — they recompute on every query rather than being pre-aggregated.
At this project's scale (12K and 493 rows respectively) this is
instant, but a production system at larger scale would materialize
these as incremental tables instead, to keep dashboard queries fast
independent of the size of the underlying fact tables.
