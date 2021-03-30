select
    location_mapping_id,
    group_id,
    subject_location_id,
    subject_organisation_id,
    creator_organisation_id,
    group_name,
    group_value,
    loaded_timestamp
from {{ ref('dim_location_group') }}