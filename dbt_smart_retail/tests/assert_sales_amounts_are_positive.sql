-- Fails (returns rows) if any sales line has a non-positive quantity, price
-- or net sales. Silver should already have quarantined these; this proves it
-- at the Gold layer, where the dashboard reads.
select transaction_line_id, quantity, unit_price, net_sales
from {{ ref('fact_sales_transaction') }}
where quantity <= 0 or unit_price <= 0 or net_sales <= 0
