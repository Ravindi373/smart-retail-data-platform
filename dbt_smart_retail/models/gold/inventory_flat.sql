-- Denormalized, dashboard-ready dataset for the stockout/low-stock
-- chart. Same rationale as sales_flat: a convenience join, materialized
-- as a view, giving Superset store/product context alongside the
-- stockout flag without a hand-joined SQL dataset per chart.

{{ config(materialized='view') }}

select
    i.snapshot_id,
    i.date_key,
    i.quantity_on_hand,
    i.reorder_point,
    i.is_stockout_risk,
    p.product_key,
    p.product_name,
    p.category,
    s.store_key,
    s.store_id,
    s.region
from {{ ref('fact_inventory_snapshot') }} i
left join {{ ref('dim_product') }} p on i.product_key = p.product_key
left join {{ ref('dim_store') }}   s on i.store_key    = s.store_key
