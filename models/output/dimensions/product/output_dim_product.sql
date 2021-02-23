select
    product_id,
    organisation_id,
    organisation_sku,
    description,
    individual_units,
    net_quantity,
    base_unit,
    brand,
    gtin,
    loaded_timestamp,
    attributes,
    is_ghost
from {{ ref('dim_product') }}