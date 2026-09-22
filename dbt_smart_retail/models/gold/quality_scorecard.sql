-- Grain: one row per source system, from the latest Silver run.
-- Complements quality_summary (which lists individual failure reasons) with
-- the overall pass rate, so a Data Reviewer can see at a glance whether each
-- source can be trusted before drilling into why rows were quarantined.

select
    source_name,
    rows_in,
    rows_clean,
    rows_quarantined,
    round(100.0 * rows_clean / nullif(rows_in, 0), 2) as pass_rate_pct,
    run_at
from {{ source('quality', 'run_summary') }}
