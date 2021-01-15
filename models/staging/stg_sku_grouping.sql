with
ranked_data as
(
select
    ORGANISATION_SKU,
    origin_organisation_number,
    business_organisation_number,
    DECISION_MAKER_ORIGIN_ORGANISATION_NUMBER,
    DECISION_MAKER_ORGANISATION_NUMBER,
    GROUPING_KEY,
    loaded_timestamp,
    row_number() over (partition by origin_organisation_number,business_organisation_number,ORGANISATION_SKU,
                                    DECISION_MAKER_ORIGIN_ORGANISATION_NUMBER,DECISION_MAKER_ORGANISATION_NUMBER order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_sku_grouping') }}
)

select *
from ranked_data
where rank = 1