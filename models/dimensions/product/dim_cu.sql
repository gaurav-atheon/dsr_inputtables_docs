{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='org_group_type_id',
        cluster_by=['loaded_timestamp']
    )
}}

select
    {{ dbt_utils.surrogate_key(['creator_origin_organisation_number','creator_business_organisation_number','group_value','loaded_timestamp']) }} as consumer_unit_id,
    --attributes
    {{ dbt_utils.surrogate_key(['creator_origin_organisation_number','creator_business_organisation_number']) }} as creator_organisation_id,
    loaded_timestamp,
    runstartedtime,
    org_group_type_id
from {{ ref('stg_sku_grouping') }}
where group_name ='product matching'
        {% if is_incremental() %}
        and loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}
group by consumer_unit_id,creator_organisation_id,loaded_timestamp,runstartedtime, org_group_type_id

