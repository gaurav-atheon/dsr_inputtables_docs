{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='org_group_type_id',
        cluster_by=['loaded_timestamp']
    )
}}
 
select
    {{ dbt_utils.surrogate_key(['subject_origin_organisation_number','subject_business_organisation_number','organisation_sku']) }} as product_id,
    {{ dbt_utils.surrogate_key(['creator_origin_organisation_number','creator_business_organisation_number']) }} as creator_organisation_id,
    case when group_name ='product matching' then
        {{ dbt_utils.surrogate_key(['creator_origin_organisation_number','creator_business_organisation_number','group_value']) }}
    else
        null
    end  as consumer_unit_id,
    group_name,
    group_value,
    attributes,
    loaded_timestamp,
    runstartedtime,
    org_group_type_id
from {{ ref('stg_sku_grouping') }}

        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}