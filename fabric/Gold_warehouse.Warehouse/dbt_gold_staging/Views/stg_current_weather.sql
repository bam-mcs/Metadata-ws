-- Auto Generated (Do not modify) CC7DAC7C68DA5FFF2A7E3BC1B1CCCF7B610CD351A21C9E064AD1784494946D97
create view [dbt_gold_staging].[stg_current_weather] as -- Thin pass-through + light renaming so downstream marts aren't coupled to source column names.
-- No heavy transformation here - that already happened in the silver notebook.

select
    location_name,
    latitude,
    longitude,
    CURRENT_TIMESTAMP as reading_at,
    cast(CURRENT_TIMESTAMP as date) as reading_date,
    condition_text,
    temperature_c_or_f as temperature_c,
    feels_like_temperature as feels_like_temperature_c,
    relative_humidity,
    uv_index,
    wind_speed,
    wind_direction_cardinal,
    precipitation_probability_pct,
    cloud_cover_pct,
    _ingested_at
from [Gold_warehouse].[weather_data].[silver_nsw_current_weather];