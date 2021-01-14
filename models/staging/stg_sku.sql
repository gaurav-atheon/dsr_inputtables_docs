with
ranked_data as
(
select
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','ORGANISATION_SKU']) }} as Product_ID,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_ID,
    ORGANISATION_SKU,
    DESCRIPTION,
    origin_organisation_number,
    business_organisation_number,
    INDIVIDUAL_UNITS,
    NET_QUANTITY,
    BASE_UNIT,
    BRAND,
    GTIN,
    row_number() over (partition by origin_organisation_number,business_organisation_number,ORGANISATION_SKU order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_sku') }}
)

select *
from ranked_data
where rank = 1