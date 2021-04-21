{{
    config(
        materialized='incremental',
        unique_key='unique_key',
        cluster_by=['loaded_timestamp']
    )
}}

select
    pln.day_date,
    pln.forecast_date,
    src.organisation_id,
    locfrom.location_id as organisation_location_id_from,
    locto.location_id as organisation_location_id_to,
    prd.product_id,
    pln.units_required,
    pln.units_required_upper,
    pln.units_required_lower,
    pln.forecast_components,
    pln.created_at,
    pln.model_version,
    pln.origin_file,
    pln.loaded_timestamp,
    {{ dbt_utils.surrogate_key(['pln.day_date','pln.forecast_date','src.organisation_id','locfrom.location_id',
                                'locto.location_id ','prd.product_id','pln.model_version']) }} as unique_key

from {{ref('stg_pln_mvt_depotstoredaysku')}} pln

inner join {{ ref('utl_source_organisations') }} src --need relationship validation earlier in the flow
on pln.source_db_id = src.business_organisation_number

inner join {{ ref('dim_location') }} locfrom --need relationship validation earlier in the flow
on locfrom.organisation_id = src.organisation_id
and locfrom.organisation_location_id = pln.organisation_location_id_from
and locfrom.location_function = 'distribution location'

inner join {{ ref('dim_location') }} locto --need relationship validation earlier in the flow
on locto.organisation_id = src.organisation_id
and locto.organisation_location_id = pln.organisation_location_id_to
and locto.location_function = 'point of sale'


inner join {{ ref('dim_product') }} prd --need relationship validation earlier in the flow
on prd.organisation_id = src.organisation_id
and prd.organisation_sku = pln.organisation_sku

        {% if is_incremental() %}
        where pln.loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}