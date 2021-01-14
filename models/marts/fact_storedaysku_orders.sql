with

Source_db as (
select organisation_id, business_organisation_number
from {{ ref('stg_organisation_mapping') }}
where origin_organisation_id = ( select organisation_id from {{ ref('utl_master_organisation') }} )
)

select
    day_date,
    src.organisation_id, --converted to DSR ID
    organisation_location_id, --convert this to DSR ID
    Organisation_SKU, --convert this to DSR ID
    epos_eaches,
    epos_value,
    rtc_epos_eaches,
    rtc_epos_value,
    rtc_reduction_value,
    bin_waste_eaches,
    bin_waste_value,
    total_waste_eaches,
    total_waste_value

from {{ ref('stg_storedaysku_orders') }} ord
inner join Source_db src
on ord.source_db_id = src.business_organisation_number