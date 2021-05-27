{{
    config(
        materialized='incremental',
        unique_key='product_id',
        cluster_by=['loaded_timestamp']
    )
}}
with all_data as (
select
    product_id,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_id,
    organisation_sku,
    description,
    individual_units,
    net_quantity,
    base_unit,
    brand,
    gtin,
    loaded_timestamp,
    attributes,
    false as is_ghost,
    runstartedtime
from {{ ref('stg_sku') }}

        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}
),
ghost_data as (
select
       ghost_data.product_id,
      ghost_data.organisation_id,
      ghost_data.organisation_sku,
      ghost_data.description,
      ghost_data.individual_units,
      ghost_data.net_quantity,
      ghost_data.base_unit,
      ghost_data.brand,
      ghost_data.gtin,
      ghost_data.loaded_timestamp,
      to_variant(ghost_data.attributes),
      ghost_data.is_ghost,
      ghost_data.runstartedtime

from {{ ref('int_all_ghost_product') }} ghost_data

where
not exists
    (select 1
    from all_data
    where all_data.product_id = ghost_data.product_id)

    {% if is_incremental() %}
    and not exists
        (select 1
        from  {{ this }} dim
        where dim.product_id = ghost_data.product_id)
        and ghost_data.runstartedtime > nvl((select max(runstartedtime) from {{ this }}), to_timestamp('0'))
    {% endif %}

)

select * from all_data
union
select * from ghost_data

