-- dim_activity.sql
select distinct
    activity_id,
    activity_name,
    activity_description,
    activity_icon_slug
from {{ ref('stg_activities') }}