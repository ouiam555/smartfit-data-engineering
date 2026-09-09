-- bridge_club_facility.sql
select distinct
    location_id,
    facility_id
from {{ ref('stg_facilities') }}