with
ranked_data as
(
select
    day_date,
    source_db_id,
    organisation_location_id,
    Organisation_SKU,
    epos_eaches,
    epos_value,
    rtc_epos_eaches,
    rtc_epos_value,
    rtc_reduction_value,
    bin_waste_eaches,
    bin_waste_value,
    total_waste_eaches,
    total_waste_value,
    loaded_timestamp,
    row_number() over (partition by day_date, source_db_id, organisation_location_id, Organisation_SKU order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_storedaysku_orders') }}
)

select *
from ranked_data
where rank = 1