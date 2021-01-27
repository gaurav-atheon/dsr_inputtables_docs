{{
    config(
        materialized='incremental',
        unique_key='unique_orgdepotdaycase_key',
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
    CASES_MATCHED_IN,
    loaded_timestamp,
           {{ dbt_utils.surrogate_key(['day_date','source_db_id','organisation_location_id','ORGANISATION_CASE']) }} as unique_orgdepotdaycase_key,
    row_number() over (partition by day_date, source_db_id, organisation_location_id, ORGANISATION_CASE order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_orgdepotdaycase_orders') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1