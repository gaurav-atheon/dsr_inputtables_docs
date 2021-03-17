{{
    config(
        materialized='incremental',
        unique_key='unique_key',
        cluster_by=['loaded_timestamp']
    )
}}

select
    day_date,
    src.organisation_id, --converted to dsr id
    loc.location_id, --converted to dsr id
    prd.product_id, --converted to dsr id
    stock_units,
    inv.loaded_timestamp,
    {{ dbt_utils.surrogate_key(['inv.day_date','src.organisation_id','loc.location_id','prd.product_id','source']) }} as unique_key,
    source

from {{ ref('stg_act_inv_locationdaysku') }} inv

inner join {{ ref('utl_source_organisations') }} src --need relationship validation earlier in the flow
on inv.source_db_id = src.business_organisation_number

inner join {{ ref('dim_location') }} loc --need relationship validation earlier in the flow
on loc.organisation_id = src.organisation_id
and loc.organisation_location_id = inv.organisation_location_id
and loc.location_function = inv.location_function

inner join {{ ref('dim_product') }} prd --need relationship validation earlier in the flow
on prd.organisation_id = src.organisation_id
and prd.organisation_sku = inv.organisation_sku

        {% if is_incremental() %}
        where inv.loaded_timestamp > (select max(loaded_timestamp) from {{ this }} where source = 'sku')
        {% endif %}

union all

select
    day_date,
    src.organisation_id, --converted to dsr id
    loc.location_id, --converted to dsr id
    prd.product_id, --converted to dsr id
    (stock_units*case_size) as stock_units,
    inv.loaded_timestamp,
    {{ dbt_utils.surrogate_key(['inv.day_date','src.organisation_id','loc.location_id','prd.product_id','source']) }} as fct_act_inv_locationdaycase_key,
    source

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
        where inv.loaded_timestamp > (select max(loaded_timestamp) from {{ this }} where source = 'case')
        {% endif %}