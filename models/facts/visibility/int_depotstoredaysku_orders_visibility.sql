select
ord.day_date,
src.organisation_id, --converted to DSR ID
prd.Product_ID as item_id, --converted to DSR ID
'fact_act_mvt_depotstoredaysku' table_reference,
'100 - Explicit' access_level,
ord.loaded_timestamp

from {{ ref('stg_act_mvt_depotstoredaysku') }} ord

inner join {{ ref('utl_source_organisations') }} src
on ord.source_db_id = src.business_organisation_number

inner join {{ ref('dim_product') }} prd
on prd.organisation_ID = src.organisation_ID
and prd.ORGANISATION_SKU = ord.ORGANISATION_SKU

group by
ord.day_date,
src.organisation_id,
prd.Product_ID,
ord.loaded_timestamp