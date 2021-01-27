{{
    config(
        materialized='incremental',
        unique_key='DAY_DATE',
        cluster_by=['loaded_timestamp']
    )
}}

select
    DAY_DATE,
    YEAR,
    MONTH,
    MONTH_NAME,
    DAY_OF_MONTH,
    DAY_OF_WEEK,
    WEEK_OF_YEAR,
    DAY_OF_YEAR,
    loaded_timestamp
from {{ ref('stg_date') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}