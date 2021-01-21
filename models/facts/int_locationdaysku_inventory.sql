select
    day_date,
    src.organisation_id, --converted to DSR ID
    loc.LOCATION_ID, --converted to DSR ID
    prd.Product_ID, --converted to DSR ID
    stock_units

from {{ ref('stg_locationdaysku_inventory') }} ord

left join {{ ref('utl_source_organisations') }} src --this should really be "inner", with relationship validation earlier in the flow
on ord.source_db_id = src.business_organisation_number

left join {{ ref('dim_location') }} loc --this should really be "inner", with relationship validation earlier in the flow
on loc.organisation_ID = src.organisation_ID
and loc.ORGANISATION_LOCATION_ID = ord.ORGANISATION_LOCATION_ID
and loc.location_function = ord.location_function

left join {{ ref('dim_product') }} prd --this should really be "inner", with relationship validation earlier in the flow
on prd.organisation_ID = src.organisation_ID
and prd.ORGANISATION_SKU = ord.ORGANISATION_SKU

