-- Grain: one row per product.

select
    product_id  as product_key,
    sku,
    name        as product_name,
    category,
    unit_cost,
    unit_price
from {{ source('silver', 'products') }}
