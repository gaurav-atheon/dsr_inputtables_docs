{{
    config(
        materialized='incremental',
        unique_key='logisticitem_id',
        cluster_by=['loaded_timestamp']
    )
}}


select
    {{ dbt_utils.surrogate_key(['creator_origin_organisation_number','creator_business_organisation_number','organisation_case']) }} as logisticitem_id,
    {{ dbt_utils.surrogate_key(['subject_origin_organisation_number','subject_business_organisation_number']) }} as creator_organisation_id,
    {{ dbt_utils.surrogate_key(['subject_origin_organisation_number','subject_business_organisation_number','grouping_key','loaded_timestamp']) }} as traded_unit_id,
    loaded_timestamp
from {{ ref('stg_case_grouping') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}