-- Denormalized, dashboard-ready dataset. Not a "real" Gold table on its
-- own — it's a convenience join over fact_sales_transaction and its
-- dimensions, materialized as a view so it's always fresh and cheap to
-- maintain. This is the single dataset Superset needs for the sales
-- charts and covers every required dashboard filter (date, channel,
-- category, store/region, loyalty tier) in one place, instead of
-- forcing every chart author to hand-join the star schema themselves.

{{ config(materialized='view') }}

select
    f.transaction_line_id,
    f.date_key,
    d.year,
    d.month,
    d.quarter,
    d.is_weekend,
    f.channel,
    f.quantity,
    f.unit_price,
    f.net_sales,
    p.product_key,
    p.product_name,
    p.category,
    s.store_key,
    s.store_id,
    s.region,
    c.customer_key,
    c.loyalty_tier
from {{ ref('fact_sales_transaction') }} f
left join {{ ref('dim_date') }}     d on f.date_key    = d.date_key
left join {{ ref('dim_product') }}  p on f.product_key = p.product_key
left join {{ ref('dim_store') }}    s on f.store_key    = s.store_key
left join {{ ref('dim_customer') }} c on f.customer_key = c.customer_key
