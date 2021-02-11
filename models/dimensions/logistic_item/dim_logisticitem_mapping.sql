{{
    config(
        materialized='incremental',
        unique_key='logisticitem_id',
        cluster_by=['loaded_timestamp']
    )
}}


select
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','organisation_case']) }} as logisticitem_id,
    {{ dbt_utils.surrogate_key(['decision_maker_origin_organisation_number','decision_maker_organisation_number']) }} as decision_maker_organisation_id,
    {{ dbt_utils.surrogate_key(['decision_maker_origin_organisation_number','decision_maker_organisation_number','grouping_key','loaded_timestamp']) }} as traded_unit_id,
    loaded_timestamp
from {{ ref('stg_case_grouping') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}