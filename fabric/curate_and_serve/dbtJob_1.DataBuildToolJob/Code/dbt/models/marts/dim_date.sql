with digits as (
    select n from (values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) as t(n)
),

spine as (
    select top (datediff(day, '2026-01-01', '2027-12-31') + 1)
        dateadd(
            day,
            row_number() over (order by (select null)) - 1,
            cast('2026-01-01' as date)
        ) as date_day
    from digits d1
    cross join digits d2
    cross join digits d3          -- 10^3 = 1,000 rows; add d4 if you widen past ~2.7 years
)

select
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_id,
    cast(date_day as date) as calendar_date,
    datepart(year, date_day) as year,
    datepart(month, date_day) as month,
    datepart(day, date_day) as day_of_month,
    cast(datename(weekday, date_day) as varchar(10)) as day_name,
    cast(datename(month,   date_day) as varchar(10)) as month_name       
from spine