-- stg_schedules.sql
select
    location_id,
    day_name,
    weekday_original,
    opening_hours
from {{ source('silver', 'schedules') }}