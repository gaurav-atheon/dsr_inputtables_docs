with
ranked_data as
(
select
    ORGANISATION_SKU,
    origin_organisation_number,
    business_organisation_number,
    ORGANISATION_CASE,
    CASE_SIZE,
    GTIN,
    row_number() over (partition by origin_organisation_number,business_organisation_number,ORGANISATION_CASE order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_case') }}
)

select *
from ranked_data
where rank = 1