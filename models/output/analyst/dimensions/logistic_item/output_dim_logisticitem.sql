select
    logisticitem_id,
    organisation_id,
    product_id,
    organisation_sku,
    organisation_case,
    case_size,
    gtin,
    loaded_timestamp,
    is_ghost,
    runstartedtime
from {{ ref('dim_logisticitem') }}