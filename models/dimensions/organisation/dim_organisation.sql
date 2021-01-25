{{
    config(
        materialized='incremental',
        unique_key='organisation_id'
    )
}}


select
    {{ dbt_utils.surrogate_key(['om.origin_organisation_number','om.business_organisation_number']) }} as organisation_id,
    om.business_organisation_name as organisation_name,
    o.type,
    o.address,
    nvl2(o.parent_organisation_number,{{ dbt_utils.surrogate_key(['o.parent_origin_organisation_number','o.parent_organisation_number']) }} ,NULL)as parent_organisation_ID,
    om.loaded_timestamp
from {{ ref('stg_organisation_mapping') }} om
left outer join {{ ref('stg_organisation') }} o
    on om.origin_organisation_number = o.origin_organisation_number
    and  om.business_organisation_number = o.business_organisation_number


        {% if is_incremental() %}
        where om.loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}