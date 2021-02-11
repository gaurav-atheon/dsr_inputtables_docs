{{
    config(
        materialized='incremental',
        unique_key='traded_unit_id',
        cluster_by=['loaded_timestamp']
    )
}}
select
    {{ dbt_utils.surrogate_key(['decision_maker_origin_organisation_number','decision_maker_organisation_number','grouping_key','loaded_timestamp']) }} as traded_unit_id,
    --attributes
    {{ dbt_utils.surrogate_key(['decision_maker_origin_organisation_number','decision_maker_organisation_number']) }} as decision_maker_organisation_id,
    loaded_timestamp
from {{ ref('stg_case_grouping') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}

group by traded_unit_id,decision_maker_organisation_id,loaded_timestamp

