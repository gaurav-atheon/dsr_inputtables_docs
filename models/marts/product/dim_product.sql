select
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','ORGANISATION_SKU']) }} as Product_ID,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_ID,
    ORGANISATION_SKU,
    DESCRIPTION,
    INDIVIDUAL_UNITS,
    NET_QUANTITY,
    BASE_UNIT,
    BRAND,
    GTIN
from {{ ref('stg_sku') }}