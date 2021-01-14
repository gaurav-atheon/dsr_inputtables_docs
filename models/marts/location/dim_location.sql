select
    LOCATION_ID,
    organisation_ID,
    ORGANISATION_LOCATION_ID,
    GEOGRAPHIC_LOCATION,
    LOCATION_FUNCTION
from {{ ref('stg_location') }}