-- stg_club_plans.sql
select
    location_id,
    plan_name
from {{ source('silver', 'club_plans') }}