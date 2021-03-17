select
    day_date,
    organisation_id,
    location_id,
    product_id,
    stock_units,
    loaded_timestamp,
    unique_key,
    source

from {{ ref('fact_act_inv_locationdaysku') }}