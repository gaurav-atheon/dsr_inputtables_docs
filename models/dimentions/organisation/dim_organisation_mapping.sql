select
{{ dbt_utils.surrogate_key(['a.origin_organisation_number','a.business_organisation_number']) }} as organisation_id,
    a.business_organisation_number,
    a.business_organisation_name,
   nvl2(a.origin_organisation_number,{{ dbt_utils.surrogate_key([ 'b.origin_organisation_number','a.origin_organisation_number']) }},NULL)as origin_organisation_id
from {{ ref('stg_organisation_mapping') }} a left outer join {{ ref('stg_organisation_mapping') }} b on a.origin_organisation_number = b.business_organisation_number
and b.origin_organisation_number = ( select business_organisation_number from {{ ref('utl_master_organisation') }} )