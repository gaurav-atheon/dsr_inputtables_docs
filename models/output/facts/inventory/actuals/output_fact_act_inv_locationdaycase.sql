select
    day_date,
    organisation_id,
    location_id,
    logisticitem_id,
    stock_units,
    case_size,
    product_id,
    loaded_timestamp,
    fct_act_inv_locationdaycase_key

from {{ ref('fact_act_inv_locationdaycase') }} inv