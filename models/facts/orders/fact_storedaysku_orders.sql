{{
    config(
        materialized='incremental',
        unique_key='unique_key'
    )
}}

select
    ord.day_date,
    src.organisation_id, --converted to DSR ID
    loc.LOCATION_ID, --converted to DSR ID
    prd.Product_ID, --converted to DSR ID
    ord.epos_eaches,
    ord.epos_value,
    ord.rtc_epos_eaches,
    ord.rtc_epos_value,
    ord.rtc_reduction_value,
    ord.bin_waste_eaches,
    ord.bin_waste_value,
    ord.total_waste_eaches,
    ord.total_waste_value,
    ord.loaded_timestamp,
    {{ dbt_utils.surrogate_key(['ord.day_date','src.organisation_id','loc.LOCATION_ID','prd.Product_ID']) }} as unique_key

from {{ref('stg_storedaysku_orders')}} ord

inner join {{ ref('utl_source_organisations') }} src --need relationship validation earlier in the flow
on ord.source_db_id = src.business_organisation_number

inner join {{ ref('dim_location') }} loc --need relationship validation earlier in the flow
on loc.organisation_ID = src.organisation_ID
and loc.ORGANISATION_LOCATION_ID = ord.ORGANISATION_LOCATION_ID
and loc.LOCATION_FUNCTION = 'Point of Sale'

inner join {{ ref('dim_product') }} prd --need relationship validation earlier in the flow
on prd.organisation_ID = src.organisation_ID
and prd.ORGANISATION_SKU = ord.ORGANISATION_SKU

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}