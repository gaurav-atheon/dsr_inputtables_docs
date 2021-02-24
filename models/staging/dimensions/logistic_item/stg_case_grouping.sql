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
    organisation_case,
    creator_origin_organisation_number,
    creator_business_organisation_number,
    subject_origin_organisation_number,
    subject_business_organisation_number,
    grouping_key,
    loaded_timestamp,
    {{ dbt_utils.surrogate_key(['creator_origin_organisation_number','creator_business_organisation_number','organisation_case',
                                'subject_origin_organisation_number','subject_business_organisation_number']) }} as logisticitem_stg_id,
    row_number() over (partition by creator_origin_organisation_number,creator_business_organisation_number,organisation_case,
                                    subject_origin_organisation_number,subject_business_organisation_number order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_case_grouping') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1