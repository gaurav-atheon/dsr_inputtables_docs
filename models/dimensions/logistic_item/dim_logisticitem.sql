{{
    config(
        materialized='incremental',
        unique_key='logisticitem_id',
        cluster_by=['loaded_timestamp']
    )
}}
with all_data as (
select
    logisticitem_id,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_id,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','ORGANISATION_SKU']) }} as product_id,
    organisation_sku,
    organisation_case,
    case_size,
    gtin,
    loaded_timestamp,
    false as is_ghost
from {{ ref('stg_case') }}

        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}
),
ghost_data as (
select
       ghost_data.logisticitem_id,
       ghost_data.organisation_id,
       ghost_data.product_id,
       ghost_data.organisation_sku,
       ghost_data.organisation_case,
       ghost_data.case_size,
       ghost_data.gtin,
       ghost_data.loaded_timestamp,
       ghost_data.is_ghost

from {{ ref('int_all_ghost_logisticitem') }} ghost_data

where
NOT EXISTS
    (select 1
    from all_data
    where all_data.logisticitem_id = ghost_data.logisticitem_id)

    {% if is_incremental() %}
    and NOT EXISTS
        (select 1
        from  {{ this }} dim
        where dim.logisticitem_id = ghost_data.logisticitem_id)
    {% endif %}

)

select * from all_data
union
select * from ghost_data

