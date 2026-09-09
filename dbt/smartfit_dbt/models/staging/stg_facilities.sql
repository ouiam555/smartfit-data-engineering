-- stg_facilities.sql
select
    location_id,
    facility_id,
    facility_name,
    facility_description,
    facility_icon_slug
from {{ source('silver', 'facilities') }}