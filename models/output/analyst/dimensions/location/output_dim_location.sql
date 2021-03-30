select
    location_id,
    organisation_id,
    organisation_location_id,
    geographic_location,
    location_function,
    attributes,
    loaded_timestamp,
    is_ghost
from {{ ref('dim_location') }}
