select
    om.organisation_ID,
    om.business_organisation_name as organisation_name,
    o.type,
    o.address,
    o.parent_organisation_ID
from {{ ref('stg_organisation_mapping') }} om
left outer join {{ ref('stg_organisation') }} o
    on om.organisation_ID = o.organisation_ID