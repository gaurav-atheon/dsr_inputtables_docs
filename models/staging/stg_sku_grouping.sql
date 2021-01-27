{{
    config(
        materialized='incremental',
        unique_key='Product_ID',
        cluster_by=['loaded_timestamp']
    )
}}
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
       {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','ORGANISATION_SKU']) }} as Product_ID,
    row_number() over (partition by origin_organisation_number,business_organisation_number,ORGANISATION_SKU,
                                    DECISION_MAKER_ORIGIN_ORGANISATION_NUMBER,DECISION_MAKER_ORGANISATION_NUMBER order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_sku_grouping') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1