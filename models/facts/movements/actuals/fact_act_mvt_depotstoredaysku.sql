{{
    config(
        materialized='incremental',
        unique_key='unique_key',
        cluster_by=['loaded_timestamp']
    )
}}

select
    day_date,
    src.organisation_id as organisation_id, --converted to dsr id
    locfrom.location_id as organisation_location_id_from, --converted to dsr id
    locto.location_id as organisation_location_id_to, --converted to dsr id
    prd.product_id, --converted to dsr id
    units_ordered,
    units_fulfilled,
    ord.loaded_timestamp,
    {{ dbt_utils.surrogate_key(['ord.day_date','src.organisation_id','locfrom.location_id','locto.location_id','prd.product_id']) }} as unique_key

from {{ ref('stg_act_mvt_depotstoredaysku') }} ord

inner join {{ ref('utl_source_organisations') }} src --need relationship validation earlier in the flow
on ord.source_db_id = src.business_organisation_number

inner join {{ ref('dim_location') }} locfrom --need relationship validation earlier in the flow
on locfrom.organisation_id = src.organisation_id
and locfrom.organisation_location_id = ord.organisation_location_id_from
and locfrom.location_function = 'distribution location'

inner join {{ ref('dim_location') }} locto --need relationship validation earlier in the flow
on locto.organisation_id = src.organisation_id
and locto.organisation_location_id = ord.organisation_location_id_to
and locto.location_function = 'point of sale'

inner join {{ ref('dim_product') }} prd --need relationship validation earlier in the flow
on prd.organisation_id = src.organisation_id
and prd.organisation_sku = ord.organisation_sku

        {% if is_incremental() %}
        where ord.loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}