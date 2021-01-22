select
    day_date,
    src.organisation_id as organisation_id, --converted to DSR ID
    locFrom.LOCATION_ID as ORGANISATION_LOCATION_ID_FROM, --converted to DSR ID
    locto.LOCATION_ID as ORGANISATION_LOCATION_ID_to, --converted to DSR ID
    prd.product_ID, --converted to DSR ID
    UNITS_ORDERED,
    UNITS_MATCHED

from {{ ref('stg_depotstoredaysku_orders') }} ord

left join {{ ref('utl_source_organisations') }} src --this should really be "inner", with relationship validation earlier in the flow
on ord.source_db_id = src.business_organisation_number

left join {{ ref('dim_location') }} locFrom --this should really be "inner", with relationship validation earlier in the flow
on locFrom.organisation_ID = src.organisation_ID
and locFrom.ORGANISATION_LOCATION_ID = ord.ORGANISATION_LOCATION_ID_FROM
and locFrom.location_function = 'Distribution Location'

left join {{ ref('dim_location') }} locto --this should really be "inner", with relationship validation earlier in the flow
on locto.organisation_ID = src.organisation_ID
and locto.ORGANISATION_LOCATION_ID = ord.ORGANISATION_LOCATION_ID_to
and locto.location_function = 'Point of Sale'

left join {{ ref('dim_product') }} prd --this should really be "inner", with relationship validation earlier in the flow
on prd.organisation_ID = src.organisation_ID
and prd.ORGANISATION_SKU = ord.ORGANISATION_SKU