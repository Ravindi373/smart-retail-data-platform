-- Every source row must be either clean or quarantined - nothing may vanish.
-- The per-source quarantine total must equal rows_quarantined in run_summary.
select s.source_name, s.rows_quarantined as expected, coalesce(q.n, 0) as actual
from {{ source('quality', 'run_summary') }} s
left join (
    select source_name, count(*) as n
    from {{ source('quality', 'quarantine') }}
    group by source_name
) q using (source_name)
where s.rows_in <> s.rows_clean + s.rows_quarantined
   or s.rows_quarantined <> coalesce(q.n, 0)
