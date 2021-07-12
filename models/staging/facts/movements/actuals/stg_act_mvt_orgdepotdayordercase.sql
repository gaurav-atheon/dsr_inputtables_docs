{{
    config(
        materialized='incremental',
        unique_key='unique_key',
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
    order_code,
    organisation_case,
    cases_ordered_in,
    cases_fulfilled_in,
    '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/London")) }}'  as runstartedtime,
    loaded_timestamp,
    created_timestamp,
    {{ dbt_utils.surrogate_key(['day_date','source_db_id','business_organisation_number_from','organisation_location_id','organisation_case']) }} as unique_key,
    row_number() over (partition by day_date, source_db_id, business_organisation_number_from,organisation_location_id, organisation_case order by loaded_timestamp desc) rank
 {% if target.name == 'ci' %}
    from {{ ref ('stg_act_mvt_orgdepotdayordercase_ci' )}}
 {% else %}
     from {{ source('dsr_input', 'input_act_mvt_orgdepotdayordercase') }}
        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}
 {% endif %}
)

select *
from ranked_data
where rank = 1