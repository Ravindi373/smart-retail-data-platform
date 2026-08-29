-- Grain: one row per physical store/warehouse location.
--
-- Known limitation: the source data (pos_sales.store_id and
-- inventory_snapshots.warehouse_id) does not carry a region field through
-- to Silver, even though the mock data generator assigns one internally
-- at generation time. region is left NULL here rather than guessed, and
-- is documented as a known gap in the final report rather than silently
-- fabricated.

with store_ids as (
    select distinct store_id as store_id from {{ source('silver', 'pos_sales') }}
    union
    select distinct warehouse_id as store_id from {{ source('silver', 'inventory_snapshots') }}
)

select
    store_id           as store_key,
    store_id,
    cast(null as text) as region,
    'in_store'         as channel
from store_ids
