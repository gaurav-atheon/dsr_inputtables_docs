{{
    config(
        materialized='incremental',
        unique_key='unique_locationdaysku_key',
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
    Organisation_SKU,
    stock_units,
    location_function,
    loaded_timestamp,
    {{ dbt_utils.surrogate_key(['day_date','source_db_id','organisation_location_id','Organisation_SKU','location_function']) }} as unique_locationdaysku_key,
    row_number() over (partition by day_date, source_db_id, organisation_location_id, Organisation_SKU,location_function order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_locationdaysku_inventory') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1