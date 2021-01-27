{{
    config(
        materialized='incremental',
        unique_key='logisticitem_ID',
        cluster_by=['loaded_timestamp']
    )
}}

select
     logisticitem_ID,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_ID,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','ORGANISATION_SKU']) }} as product_ID,
    ORGANISATION_SKU,
    ORGANISATION_CASE,
    CASE_SIZE,
    GTIN,
    loaded_timestamp
from {{ ref('stg_case') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}