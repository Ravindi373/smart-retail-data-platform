-- Grain: one row per customer.
-- PII was already hashed in Silver; this model never re-exposes raw
-- email/phone. Customers with no loyalty programme membership have a NULL
-- tier in the source; they are labelled 'none' here so the dashboard's
-- loyalty-tier filter has an explicit value to select instead of a blank.

select
    customer_id                       as customer_key,
    name,
    email_hash,
    phone_hash,
    coalesce(loyalty_tier, 'none')    as loyalty_tier,
    created_at
from {{ source('silver', 'customers') }}
