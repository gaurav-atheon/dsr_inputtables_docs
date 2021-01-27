{{
    config(
        materialized='incremental',
        unique_key='Product_ID',
        cluster_by=['loaded_timestamp']
    )
}}
select
    Product_ID,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_ID,
    ORGANISATION_SKU,
    DESCRIPTION,
    INDIVIDUAL_UNITS,
    NET_QUANTITY,
    BASE_UNIT,
    BRAND,
    GTIN,
    loaded_timestamp
from {{ ref('stg_sku') }}


        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}