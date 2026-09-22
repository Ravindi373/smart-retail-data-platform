-- The dashboard view must add columns, never change the grain: same row
-- count as the fact table it is built on. Returns a row only on mismatch.
select f.n as fact_rows, v.n as view_rows
from (select count(*) as n from {{ ref('fact_sales_transaction') }}) f
cross join (select count(*) as n from {{ ref('sales_flat') }}) v
where f.n <> v.n
