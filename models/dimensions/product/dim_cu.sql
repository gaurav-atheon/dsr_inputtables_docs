{{
    config(
        materialized='incremental',
        unique_key='consumer_unit_id',
        cluster_by=['loaded_timestamp']
    )
}}

select
    {{ dbt_utils.surrogate_key(['decision_maker_origin_organisation_number','decision_maker_organisation_number','grouping_key','loaded_timestamp']) }} as consumer_unit_id,
    --attributes
    {{ dbt_utils.surrogate_key(['decision_maker_origin_organisation_number','decision_maker_organisation_number']) }} as decision_maker_organisation_id,
    loaded_timestamp
from {{ ref('stg_sku_grouping') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
group by consumer_unit_id,decision_maker_organisation_id,loaded_timestamp

