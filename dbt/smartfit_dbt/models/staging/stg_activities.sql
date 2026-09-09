-- stg_activities.sql
select
    location_id,
    activity_id,
    activity_name,
    activity_description,
    activity_icon_slug
from {{ source('silver', 'activities') }}