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
    row_number() over (partition by day_date, source_db_id, organisation_location_id, Organisation_SKU,location_function order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_locationdaysku_inventory') }}
)

select *
from ranked_data
where rank = 1