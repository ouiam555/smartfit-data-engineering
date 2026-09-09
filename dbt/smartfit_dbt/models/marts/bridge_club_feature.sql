-- bridge_club_feature.sql
select distinct
    location_id,
    feature_id
from {{ ref('stg_features') }}