{{
    config(
        materialized='incremental',
        unique_key='Product_ID',
        cluster_by=['loaded_timestamp']
    )
}}
with all_data as (
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
    loaded_timestamp,
    false as is_ghost
from {{ ref('stg_sku') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
),
ghost_data as (
select
       ghost_data.Product_ID,ghost_data.organisation_id,ghost_data.ORGANISATION_SKU,ghost_data.DESCRIPTION,
       ghost_data.INDIVIDUAL_UNITS,ghost_data.NET_QUANTITY, ghost_data.BASE_UNIT,ghost_data.BRAND,ghost_data.GTIN,
       ghost_data.loaded_timestamp,ghost_data.is_ghost

from {{ ref('int_all_ghost_product') }} ghost_data

where
NOT EXISTS
    (select 1
    from all_data
    where all_data.Product_ID = ghost_data.Product_ID)

    {% if is_incremental() %}
    and NOT EXISTS
        (select 1
        from  {{ this }} dim
        where dim.Product_ID = all_data.Product_ID)
    {% endif %}

)

select * from all_data
union
select * from ghost_data

