{{
    config(
        materialized='incremental',
        unique_key='logisticitem_ID',
        cluster_by=['loaded_timestamp']
    )
}}
with all_data as (
select
     logisticitem_ID,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_ID,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','ORGANISATION_SKU']) }} as product_ID,
    ORGANISATION_SKU,
    ORGANISATION_CASE,
    CASE_SIZE,
    GTIN,
    loaded_timestamp,
    false as is_ghost
from {{ ref('stg_case') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
),
ghost_data as (
select
        ghost_data.logisticitem_ID,ghost_data.organisation_ID,ghost_data.product_ID,ghost_data.ORGANISATION_SKU,ghost_data.ORGANISATION_CASE,
        ghost_data.CASE_SIZE,ghost_data.GTIN, ghost_data.loaded_timestamp,ghost_data.is_ghost

from {{ ref('int_all_ghost_logisticitem') }} ghost_data
left join all_data
on all_data.logisticitem_ID = ghost_data.logisticitem_ID
  left join  {{ this }} prd
        on prd.logisticitem_ID = all_data.logisticitem_ID

where  prd.logisticitem_ID is null
and all_data.logisticitem_ID is null
)

select * from all_data
union
select * from ghost_data

