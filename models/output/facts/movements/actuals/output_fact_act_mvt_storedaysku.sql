select
    day_date,
    organisation_id,
    location_id,
    product_id,
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
    unique_key

from {{ref('fact_act_mvt_storedaysku')}}