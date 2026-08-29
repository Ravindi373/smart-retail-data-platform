-- Grain: one row per (source_name, failed_rule) combination.
-- Powers the "data quality summary" dashboard requirement (Week 6) —
-- lets a Data Reviewer see pipeline health at a glance without querying
-- quality.quarantine directly.

select
    source_name,
    failed_rule,
    count(*)             as quarantined_count,
    min(quarantined_at)  as first_seen,
    max(quarantined_at)  as last_seen
from {{ source('quality', 'quarantine') }}
group by source_name, failed_rule
order by source_name, failed_rule
