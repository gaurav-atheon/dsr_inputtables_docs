{{
    config(
        materialized='incremental',
        unique_key='day_date',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    DAY_DATE,
    WEEK_ID,
    YEAR_ID,
    origin_organisation_number,
    business_organisation_number,
    loaded_timestamp,
    row_number() over (partition by origin_organisation_number,business_organisation_number,DAY_DATE order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_calendar') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1