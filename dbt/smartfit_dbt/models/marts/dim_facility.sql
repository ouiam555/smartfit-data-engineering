-- dim_facility.sql
select distinct
    facility_id,
    facility_name,
    facility_description,
    facility_icon_slug
from {{ ref('stg_facilities') }}