select
    day_date,
    organisation_id,
    location_id,
    product_id,
    stock_units,
    stock_value,
    loaded_timestamp,
    unique_key,
    source,
    runstartedtime

from {{ ref('fact_act_inv_locationdaysku') }}