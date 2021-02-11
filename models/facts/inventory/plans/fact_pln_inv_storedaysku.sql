{{
    config(
        materialized='incremental',
        unique_key='fct_act_mvt_storedaysku_key',
        cluster_by=['loaded_timestamp']
    )
}}

select
    ord.day_date,
    src.organisation_id, --converted to dsr id
    loc.location_id, --converted to dsr id
    prd.product_id, --converted to dsr id
    ord.ranged,
    ord.loaded_timestamp,
    {{ dbt_utils.surrogate_key(['ord.day_date','src.organisation_id','loc.location_id','prd.product_id']) }} as fct_act_mvt_storedaysku_key

from {{ref('stg_pln_inv_storedaysku')}} ord

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
        where ord.loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}