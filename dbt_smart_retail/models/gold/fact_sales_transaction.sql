-- Grain: one row per sales line item (one product within one
-- transaction/order), combining both channels:
--   - in-store: one row per pos_sales record
--   - online: one row per item inside an ecommerce_orders.items array,
--     unnested here since Silver stores it as a JSON array per order
--
-- store_key is intentionally NULL for online sales_transaction rows —
-- an online order has no physical store, so this is a legitimate NULL,
-- not a missing-data problem. The relationships test on store_key in
-- schema.yml only checks non-null values for exactly this reason.

with pos_lines as (

    select
        transaction_id                  as transaction_line_id,
        customer_id                     as customer_key,
        product_id                      as product_key,
        store_id                        as store_key,
        txn_timestamp::date             as date_key,
        quantity,
        unit_price,
        (quantity * unit_price)::numeric as net_sales,
        'in_store'                      as channel
    from {{ source('silver', 'pos_sales') }}

),

ecommerce_lines as (

    select
        o.order_id || '-' || item.idx                                  as transaction_line_id,
        o.customer_id                                                   as customer_key,
        item.value ->> 'product_id'                                     as product_key,
        cast(null as text)                                              as store_key,
        o.order_timestamp::date                                         as date_key,
        (item.value ->> 'quantity')::int                                as quantity,
        (item.value ->> 'unit_price')::numeric                          as unit_price,
        ((item.value ->> 'quantity')::int * (item.value ->> 'unit_price')::numeric) as net_sales,
        o.channel
    from {{ source('silver', 'ecommerce_orders') }} o,
         jsonb_array_elements(o.items) with ordinality as item(value, idx)

)

select * from pos_lines
union all
select * from ecommerce_lines
