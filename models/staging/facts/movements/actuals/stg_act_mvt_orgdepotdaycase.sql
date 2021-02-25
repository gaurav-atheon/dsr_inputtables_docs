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
    business_organisation_number_from,
    organisation_case,
    cases_ordered_in,
    cases_fulfilled_in,
    loaded_timestamp,
    created_timestamp,
    {{ dbt_utils.surrogate_key(['day_date','source_db_id','business_organisation_number_from','organisation_location_id','organisation_case']) }} as unique_act_mvt_orgdepotdaycase,
    row_number() over (partition by day_date, source_db_id, organisation_location_id, organisation_case order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_act_mvt_orgdepotdaycase') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1