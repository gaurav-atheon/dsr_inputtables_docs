{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='unq_calendar_id',
        cluster_by=['loaded_timestamp']
    )
}}
select
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_id,
    day_date,
    week_id,
    year_id,
    MIN(day_date) OVER (
                PARTITION BY
                    week_id,
                    year_id)  AS weekcommencing,
    MAX(day_date) OVER (
               PARTITION BY
                   week_id,
                   year_id)   AS weekending,
    DAYOFWEEKISO(day_date)    AS dayofweeknumber, --  week starts on monday = day 1 using ISO semantics
    RANK() OVER (
               PARTITION BY
                   year_id
               ORDER BY
                   day_date)  AS day,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','day_date']) }} as unq_calendar_id,
    loaded_timestamp,
    runstartedtime
from {{ ref('stg_calendar') }}

        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}