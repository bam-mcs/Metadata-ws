-- Current-conditions readings are point-in-time snapshots (potentially several per day
-- depending on how often the bronze notebook runs). This rolls them up to one row per
-- location per day so it can join cleanly with the daily-grain forecast and air-quality facts.

with daily_agg as (
    select
        location_name,
        reading_date,
        avg(temperature_c) as avg_temperature_c,
        min(temperature_c) as min_temperature_c,
        max(temperature_c) as max_temperature_c,
        avg(relative_humidity) as avg_relative_humidity,
        avg(wind_speed) as avg_wind_speed,
        max(uv_index) as max_uv_index,
        avg(cloud_cover_pct) as avg_cloud_cover_pct,
        count(*) as reading_count
    from {{ ref('stg_current_weather') }}
    group by location_name, reading_date
),

-- "Condition of the day" = whatever condition was reported on the latest reading of that day,
-- rather than trying to average a categorical text field.
latest_condition as (
    select
        location_name,
        reading_date,
        condition_text,
        row_number() over (
            partition by location_name, reading_date
            order by reading_at desc
        ) as rn
    from {{ ref('stg_current_weather') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['a.location_name']) }} as location_id,
    {{ dbt_utils.generate_surrogate_key(['a.reading_date']) }} as date_id,
    a.location_name,
    a.reading_date as calendar_date,
    a.avg_temperature_c,
    a.min_temperature_c,
    a.max_temperature_c,
    a.avg_relative_humidity,
    a.avg_wind_speed,
    a.max_uv_index,
    a.avg_cloud_cover_pct,
    a.reading_count,
    c.condition_text as end_of_day_condition_text
from daily_agg a
left join latest_condition c
    on a.location_name = c.location_name
    and a.reading_date = c.reading_date
    and c.rn = 1
