{{
    config(
        materialized='incremental',
        unique_key='unique_depotstoredaysku_key',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    day_date,
    source_db_id,
    ORGANISATION_LOCATION_ID_FROM,
    ORGANISATION_LOCATION_ID_TO,
    ORGANISATION_sku,
    UNITS_ORDERED,
    UNITS_MATCHED,
    loaded_timestamp,
       {{ dbt_utils.surrogate_key(['day_date','source_db_id','ORGANISATION_LOCATION_ID_FROM','ORGANISATION_LOCATION_ID_TO','ORGANISATION_sku']) }} as unique_depotstoredaysku_key,
    row_number() over (partition by day_date, source_db_id, ORGANISATION_LOCATION_ID_FROM,ORGANISATION_LOCATION_ID_TO, ORGANISATION_sku order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_depotstoredaysku_orders') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1