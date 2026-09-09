-- dim_feature.sql
select distinct
    feature_id,
    feature_name,
    feature_description,
    feature_icon_slug
from {{ ref('stg_features') }}