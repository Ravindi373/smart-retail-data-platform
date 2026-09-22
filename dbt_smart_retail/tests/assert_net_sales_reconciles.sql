-- Fails if net_sales is not quantity x unit_price for any line.
select transaction_line_id, quantity, unit_price, net_sales
from {{ ref('fact_sales_transaction') }}
where abs(net_sales - (quantity * unit_price)) > 0.0001
