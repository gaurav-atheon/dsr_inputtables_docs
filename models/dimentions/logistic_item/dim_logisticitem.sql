select
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','ORGANISATION_CASE']) }} as logisticitem_ID,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_ID,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','ORGANISATION_SKU']) }} as product_ID,
    ORGANISATION_SKU,
    ORGANISATION_CASE,
    CASE_SIZE,
    GTIN
from {{ ref('stg_case') }}