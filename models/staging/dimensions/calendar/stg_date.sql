{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='day_date',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    day_date,
    year,
    month,
    month_name,
    day_of_month,
    day_of_week,
    week_of_year,
    day_of_year,
    loaded_timestamp,
    created_timestamp,
    '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/London")) }}'  as runstartedtime,
    row_number() over (partition by day_date order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_date') }}
        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}
)

select *
from ranked_data
where rank = 1