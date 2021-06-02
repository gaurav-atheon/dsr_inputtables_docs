{{
    config(
        materialized='incremental',
        unique_key='parentage_id',
        cluster_by=['loaded_timestamp']
    )
}}
select
      {{ dbt_utils.surrogate_key(['creator_origin_organisation_number','creator_business_organisation_number']) }} as creator_organisation_id,
      {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as subject_organisation_id,
      {{ dbt_utils.surrogate_key(['parent_origin_organisation_number','parent_business_organisation_number']) }} as parent_organisation_id,
      {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number',
                                'creator_origin_organisation_number','creator_business_organisation_number']) }} as parentage_id,
      loaded_timestamp,
      runstartedtime
from {{ ref('stg_organisation_parentage') }}

        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}
