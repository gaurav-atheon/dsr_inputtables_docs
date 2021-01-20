select {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_id,business_organisation_number,origin_organisation_number
from  {{ ref('master_organisation') }}
where origin_organisation_number is null