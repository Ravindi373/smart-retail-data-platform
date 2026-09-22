-- Grain: one row per physical store/warehouse location.
--
-- Built from the store master (silver.stores), which carries the region.
-- Store ids that appear in the fact sources but are missing from the master
-- are still included (with a NULL region) so no fact row loses its
-- dimension row - and the not_null test on region then fails loudly instead
-- of the gap being hidden.

with store_ids as (
    select store_id from {{ source('silver', 'stores') }}
    union
    select store_id from {{ source('silver', 'pos_sales') }}
    union
    select warehouse_id as store_id from {{ source('silver', 'inventory_snapshots') }}
)

select
    ids.store_id                         as store_key,
    ids.store_id,
    st.region,
    coalesce(st.channel, 'in_store')     as channel
from store_ids ids
left join {{ source('silver', 'stores') }} st
    on st.store_id = ids.store_id
