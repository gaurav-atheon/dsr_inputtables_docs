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
    DESCRIPTION,
    origin_organisation_number,
    business_organisation_number,
    INDIVIDUAL_UNITS,
    NET_QUANTITY,
    BASE_UNIT,
    BRAND,
    GTIN,
    loaded_timestamp,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','ORGANISATION_SKU']) }} as Product_ID,
    row_number() over (partition by origin_organisation_number,business_organisation_number,ORGANISATION_SKU order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_sku') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1