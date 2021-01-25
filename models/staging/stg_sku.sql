with
ranked_data as
(
select
    ORGANISATION_SKU,
    DESCRIPTION,
    origin_organisation_number,
    business_organisation_number,
    INDIVIDUAL_UNITS,
    NET_QUANTITY,
    BASE_UNIT,
    BRAND,
    GTIN,
    loaded_timestamp,
    row_number() over (partition by origin_organisation_number,business_organisation_number,ORGANISATION_SKU order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_sku') }}
)

select *
from ranked_data
where rank = 1