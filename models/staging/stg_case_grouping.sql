{{
    config(
        materialized='incremental',
        unique_key='logisticitem_stg_id',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    ORGANISATION_CASE,
    origin_organisation_number,
    business_organisation_number,
    DECISION_MAKER_ORIGIN_ORGANISATION_NUMBER,
    DECISION_MAKER_ORGANISATION_NUMBER,
    GROUPING_KEY,
    loaded_timestamp,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','ORGANISATION_CASE',
                                'DECISION_MAKER_ORIGIN_ORGANISATION_NUMBER','DECISION_MAKER_ORGANISATION_NUMBER']) }} as logisticitem_stg_id,
    row_number() over (partition by origin_organisation_number,business_organisation_number,ORGANISATION_CASE,
                                    DECISION_MAKER_ORIGIN_ORGANISATION_NUMBER,DECISION_MAKER_ORGANISATION_NUMBER order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_case_grouping') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1