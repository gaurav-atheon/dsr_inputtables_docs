{{
    config(
        materialized='incremental',
        unique_key='traded_unit_id',
        cluster_by=['loaded_timestamp']
    )
}}
select
    {{ dbt_utils.surrogate_key(['DECISION_MAKER_ORIGIN_ORGANISATION_NUMBER','DECISION_MAKER_ORGANISATION_NUMBER','GROUPING_KEY','loaded_timestamp']) }} as traded_unit_id,
    --attributes
    {{ dbt_utils.surrogate_key(['DECISION_MAKER_ORIGIN_ORGANISATION_NUMBER','DECISION_MAKER_ORGANISATION_NUMBER']) }} as Decision_maker_organisation_ID,
    loaded_timestamp
from {{ ref('stg_case_grouping') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}

group by traded_unit_id,Decision_maker_organisation_ID,loaded_timestamp

