select
    organisation_id,
    business_organisation_number,
    business_organisation_name,
    origin_organisation_id
from {{ ref('stg_organisation_mapping') }}