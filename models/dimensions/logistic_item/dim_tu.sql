{{
    config(
        materialized='incremental',
        unique_key='traded_unit_id',
        cluster_by=['loaded_timestamp']
    )
}}
select
    {{ dbt_utils.surrogate_key(['creator_origin_organisation_number','creator_business_organisation_number','grouping_key','loaded_timestamp']) }} as traded_unit_id,
    --attributes
    {{ dbt_utils.surrogate_key(['creator_origin_organisation_number','creator_business_organisation_number']) }} as creator_organisation_id,
    loaded_timestamp,
    runstartedtime
from {{ ref('stg_case_grouping') }}

        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}

group by traded_unit_id,creator_organisation_id,loaded_timestamp,runstartedtime

