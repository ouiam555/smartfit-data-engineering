select
    location_id,
    day_name,
    opening_hours
from {{ ref('stg_schedules') }}