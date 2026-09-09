-- bridge_club_plan.sql
select distinct
    location_id,
    plan_name
from {{ ref('stg_club_plans') }}