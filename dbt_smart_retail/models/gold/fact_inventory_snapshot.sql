-- Grain: one row per product, per store/warehouse, per snapshot date.
-- is_stockout_risk is derived, not sourced directly: true when stock on
-- hand has fallen to or below the reorder point.

select
    product_id || '_' || warehouse_id || '_' || snapshot_date as snapshot_id,
    snapshot_date                                              as date_key,
    product_id                                                 as product_key,
    warehouse_id                                                as store_key,
    quantity_on_hand,
    reorder_point,
    (quantity_on_hand <= reorder_point)                        as is_stockout_risk
from {{ source('silver', 'inventory_snapshots') }}
