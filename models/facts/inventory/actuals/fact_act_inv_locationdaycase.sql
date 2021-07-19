{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='unique_key',
        cluster_by=['loaded_timestamp']
    )
}}

select
    day_date,
    src.organisation_id, --converted to dsr id
    loc.location_id, --converted to dsr id
    prd.logisticitem_id, --converted to dsr id
    stock_units,
    stock_value,
    prd.case_size,
    prd.product_id,
    inv.loaded_timestamp,
    '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/London")) }}'  as runstartedtime,
    {{ dbt_utils.surrogate_key(['inv.day_date','src.organisation_id','loc.location_id','prd.logisticitem_id']) }} as unique_key

from {{ ref('stg_act_inv_locationdaycase') }} inv

inner join {{ ref('utl_source_organisations') }} src --need relationship validation earlier in the flow
on inv.source_db_id = src.business_organisation_number

inner join {{ ref('dim_location') }} loc --need relationship validation earlier in the flow
on loc.organisation_id = src.organisation_id
and loc.organisation_location_id = inv.organisation_location_id
and loc.location_function = inv.location_function

inner join {{ ref('dim_logisticitem') }} prd --need relationship validation earlier in the flow
on prd.organisation_id = src.organisation_id
and prd.organisation_case = inv.organisation_case

        {% if is_incremental() %}
        where inv.loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}