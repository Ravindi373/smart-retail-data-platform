-- Grain: one row per calendar date.
-- A static, wide range (2020-2030) safely covers every date that
-- appears in the mock dataset (generated relative to a fixed anchor
-- date, with lookback/lookahead of at most a few years). A production
-- system at larger scale would use a narrower, dynamically-computed
-- range instead.

with date_spine as (
    select generate_series(
        date '2020-01-01',
        date '2030-12-31',
        interval '1 day'
    )::date as date_day
)

select
    date_day                                as date_key,
    extract(year from date_day)::int        as year,
    extract(month from date_day)::int       as month,
    extract(day from date_day)::int         as day,
    extract(quarter from date_day)::int     as quarter,
    to_char(date_day, 'Day')                as day_of_week,
    extract(isodow from date_day) in (6, 7) as is_weekend
from date_spine
