-- One row per NSW location.

with deduped as (
    select
        location_name,
        max(latitude) as latitude,
        max(longitude) as longitude
    from {{ ref('stg_current_weather') }}
    group by location_name
)

select
    {{ dbt_utils.generate_surrogate_key(['location_name']) }} as location_id,
    location_name,
    latitude,
    longitude
from deduped
