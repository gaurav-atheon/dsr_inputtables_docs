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
    row_number() over (partition by day_date, source_db_id, ORGANISATION_LOCATION_ID_FROM,ORGANISATION_LOCATION_ID_TO, ORGANISATION_sku order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_depotstoredaysku_orders') }}
)

select *
from ranked_data
where rank = 1