{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='unique_key',
        cluster_by=['loaded_timestamp']
    )
}}

select
    ord.day_date,
    src.organisation_id, --converted to dsr id
    loc.location_id, --converted to dsr id
    prd.product_id, --converted to dsr id
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
    '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/London")) }}'  as runstartedtime,
    {{ dbt_utils.surrogate_key(['ord.day_date','src.organisation_id','loc.location_id','prd.product_id']) }} as unique_key

from {{ref('stg_act_mvt_storedaysku')}} ord

inner join {{ ref('utl_source_organisations') }} src --need relationship validation earlier in the flow
on ord.source_db_id = src.business_organisation_number

inner join {{ ref('dim_location') }} loc --need relationship validation earlier in the flow
on loc.organisation_id = src.organisation_id
and loc.organisation_location_id = ord.organisation_location_id
and loc.location_function = 'point of sale'

inner join {{ ref('dim_product') }} prd --need relationship validation earlier in the flow
on prd.organisation_id = src.organisation_id
and prd.organisation_sku = ord.organisation_sku

        {% if is_incremental() %}
        where ord.loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}