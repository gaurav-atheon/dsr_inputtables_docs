with

Source_db as (
select organisation_id, business_organisation_number
from {{ ref('stg_organisation_mapping') }}
where origin_organisation_id = ( select organisation_id from {{ ref('utl_master_organisation') }} )
)

select
ord.day_date,
src.organisation_id, --converted to DSR ID
Organisation_SKU,
'fact_storedaysku_orders' table_reference,
'100 - Explicit' access_level

from {{ ref('stg_storedaysku_orders') }} ord
inner join Source_db src
on ord.source_db_id = src.business_organisation_number