select
    organisation_id,
    origin_organisation_number,
    business_organisation_number,
    business_organisation_name,
    origin_organisation_id,
    loaded_timestamp,
    runstartedtime
from {{ ref('dim_organisation_mapping') }}
