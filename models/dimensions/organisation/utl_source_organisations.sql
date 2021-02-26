select organisation_id, business_organisation_number,origin_organisation_number,origin_organisation_id
from {{ ref('dim_organisation_mapping') }}
where
origin_organisation_id = ( select organisation_id from {{ ref('utl_master_organisation') }} )
or
origin_organisation_id is null