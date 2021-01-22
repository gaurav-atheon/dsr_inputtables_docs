select
ord.day_date,
src.organisation_id, --converted to DSR ID
prd.logisticitem_ID as item_id, --converted to DSR ID
'fact_orgdepotdaycase_orders' table_reference,
'100 - Explicit' access_level,
loaded_timestamp

from {{ ref('stg_orgdepotdaycase_orders') }} ord

inner join {{ ref('utl_source_organisations') }} src
on ord.source_db_id = src.business_organisation_number

inner join {{ ref('dim_logisticitem') }} prd --this should really be "inner", with relationship validation earlier in the flow
on prd.organisation_ID = src.organisation_ID
and prd.ORGANISATION_case = ord.ORGANISATION_case

group by
ord.day_date,
src.organisation_id,
prd.logisticitem_ID,
loaded_timestamp