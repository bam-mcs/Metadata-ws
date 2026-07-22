-- Thin pass-through + light renaming so downstream marts aren't coupled to source column names.
-- No heavy transformation here - that already happened in the silver notebook.

select
    location_name,
    latitude,
    longitude,
    current_time as reading_at,
    cast(current_time as date) as reading_date,
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
from {{ source('silver', 'silver_nsw_current_weather') }}
