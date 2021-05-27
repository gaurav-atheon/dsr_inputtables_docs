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
    day_date,
    week_id,
    year_id,
    origin_organisation_number,
    business_organisation_number,
    loaded_timestamp,
    created_timestamp,
    '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/London")) }}'  as runstartedtime,
    row_number() over (partition by origin_organisation_number,business_organisation_number,day_date order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_calendar') }}
        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}
)

select *
from ranked_data
where rank = 1