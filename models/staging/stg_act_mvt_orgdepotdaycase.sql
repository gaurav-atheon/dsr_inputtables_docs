{{
    config(
        materialized='incremental',
        unique_key='unique_act_mvt_orgdepotdaycase',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    day_date,
    source_db_id,
    organisation_location_id,
    BUSINESS_ORGANISATION_NUMBER_FROM,
    ORGANISATION_CASE,
    CASES_ORDERED_IN,
    CASES_FULFILLED_IN,
    loaded_timestamp,
           {{ dbt_utils.surrogate_key(['day_date','source_db_id','organisation_location_id','ORGANISATION_CASE']) }} as unique_act_mvt_orgdepotdaycase,
    row_number() over (partition by day_date, source_db_id, organisation_location_id, ORGANISATION_CASE order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_act_mvt_orgdepotdaycase') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1