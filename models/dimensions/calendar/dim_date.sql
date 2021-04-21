{{
    config(
        materialized='incremental',
        unique_key='day_date',
        cluster_by=['loaded_timestamp']
    )
}}

select
    day_date,
    year,
    month,
    month_name,
    day_of_month,
    day_of_week,
    week_of_year,
    day_of_year,
    loaded_timestamp
from {{ ref('stg_date') }}

        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}