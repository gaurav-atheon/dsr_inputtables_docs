with
ranked_data as
(
select
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','ORGANISATION_SKU']) }} as Product_ID,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_ID,
    ORGANISATION_SKU,
    origin_organisation_number,
    business_organisation_number,
    DECISION_MAKER_ORIGIN_ORGANISATION_NUMBER,
    DECISION_MAKER_ORGANISATION_NUMBER,
    GROUPING_KEY,
    row_number() over (partition by origin_organisation_number,business_organisation_number,ORGANISATION_SKU,
                                    DECISION_MAKER_ORIGIN_ORGANISATION_NUMBER,DECISION_MAKER_ORGANISATION_NUMBER order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_sku_grouping') }}
)

select *
from ranked_data
where rank = 1