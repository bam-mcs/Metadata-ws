-- Static calendar spine. dbt_utils.date_spine needs compile-time bounds, so this range is
-- fixed rather than derived from your data - widen START_DATE/END_DATE as needed, or swap
-- this for a proper date dimension package if you already use one elsewhere.

with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2026-01-01' as date)",
        end_date="cast('2027-12-31' as date)"
    ) }}
)

select
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_id,
    cast(date_day as date) as calendar_date,
    datepart(year, date_day) as year,
    datepart(month, date_day) as month,
    datepart(day, date_day) as day_of_month,
    datepart(weekday, date_day) as day_of_week,
    format(date_day, 'dddd') as day_name,
    format(date_day, 'MMMM') as month_name
from spine
