{{
    config(
        materialized='incremental',
        unique_key='location_mapping_id',
        cluster_by=['loaded_timestamp']
    )
}}

select
    location_mapping_id,
    {{ dbt_utils.surrogate_key(['creator_origin_organisation_number','creator_business_organisation_number','group_name']) }} as group_id,
    {{ dbt_utils.surrogate_key(['subject_origin_organisation_number','subject_business_organisation_number','organisation_location_id','location_function']) }} as subject_location_id,
    {{ dbt_utils.surrogate_key(['subject_origin_organisation_number','subject_business_organisation_number']) }} as subject_organisation_id,
    {{ dbt_utils.surrogate_key(['creator_origin_organisation_number','creator_business_organisation_number']) }} as creator_organisation_id,
    group_name,
    group_value,
    attributes,
    loaded_timestamp
from {{ ref('stg_location_group') }}

        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}