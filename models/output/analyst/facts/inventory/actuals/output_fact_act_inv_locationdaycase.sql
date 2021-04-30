select
    day_date,
    organisation_id,
    location_id,
    logisticitem_id,
    stock_units,
    stock_value,
    case_size,
    product_id,
    loaded_timestamp,
    unique_key

from {{ ref('fact_act_inv_locationdaycase') }} inv