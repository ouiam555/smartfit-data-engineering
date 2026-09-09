-- dim_plan.sql
select distinct
    plan_name
from {{ ref('stg_club_plans') }}