{{
    config(
        materialized='incremental',
        unique_key='DAY_DATE',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    DAY_DATE,
    YEAR,
    MONTH,
    MONTH_NAME,
    DAY_OF_MONTH,
    DAY_OF_WEEK,
    WEEK_OF_YEAR,
    DAY_OF_YEAR,
    loaded_timestamp,
    row_number() over (partition by DAY_DATE order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_date') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1