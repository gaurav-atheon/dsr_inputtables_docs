{{
    config(
        materialized='incremental',
        unique_key='organisation_id',
        cluster_by=['loaded_timestamp']
    )
}}

select
    a.organisation_id,
    a.origin_organisation_number,
    a.business_organisation_number,
    a.business_organisation_name,
   nvl2(a.origin_organisation_number,{{ dbt_utils.surrogate_key([ 'b.origin_organisation_number','a.origin_organisation_number']) }},null) as origin_organisation_id,
   a.loaded_timestamp
from {{ ref('stg_organisation_mapping') }} a
left outer join {{ ref('stg_organisation_mapping') }} b
on a.origin_organisation_number = b.business_organisation_number
and to_number(b.origin_organisation_number) = ( select business_organisation_number from {{ ref('utl_master_organisation') }} )

        {% if is_incremental() %}
        where a.loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}