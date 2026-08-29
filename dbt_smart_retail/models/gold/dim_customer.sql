-- Grain: one row per customer.
-- PII was already hashed in Silver; this model never re-exposes raw
-- email/phone.

select
    customer_id           as customer_key,
    name,
    email_hash,
    phone_hash,
    loyalty_tier,
    created_at
from {{ source('silver', 'customers') }}
