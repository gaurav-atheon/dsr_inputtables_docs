select distinct do.organisation_name, business_organisation_number, dom.origin_organisation_number
from {{ ref('output_dim_organisation') }} do
inner join {{ref('output_dim_organisation_mapping')}} dom on do.organisation_id = dom.organisation_id