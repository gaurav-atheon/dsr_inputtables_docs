select
    Product_ID,
    organisation_ID,
    ORGANISATION_SKU,
    DESCRIPTION,
    INDIVIDUAL_UNITS,
    NET_QUANTITY,
    BASE_UNIT,
    BRAND,
    GTIN
from {{ ref('stg_sku') }}