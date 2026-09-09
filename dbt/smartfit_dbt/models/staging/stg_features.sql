-- stg_features.sql
select
    location_id,
    feature_id,
    feature_name,
    feature_description,
    feature_icon_slug
from {{ source('silver', 'features') }}