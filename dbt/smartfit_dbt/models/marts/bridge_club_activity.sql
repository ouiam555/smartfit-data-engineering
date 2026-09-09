-- bridge_club_activity.sql
select distinct
    location_id,
    activity_id
from {{ ref('stg_activities') }}