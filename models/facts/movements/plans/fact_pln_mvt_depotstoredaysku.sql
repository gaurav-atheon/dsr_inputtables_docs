{{
    config(
        materialized='incremental',
        unique_key='fct_pln_inv_storedaysku_key',
        cluster_by=['loaded_timestamp']
    )
}}

select
    pln.base_forecast_date,
    pln.forecast_date,
    pln.source_db_id,
    locFrom.LOCATION_ID as organisation_location_id_from,
    locto.LOCATION_ID as organisation_location_id_to,
    pln.organisation_sku,
    pln.units_required,
    pln.units_required_upper,
    pln.units_required_lower,
    pln.forecast_components,
    pln.created_at,
    pln.model_version,
    pln.origin_file,
    pln.loaded_timestamp,
    {{ dbt_utils.surrogate_key(['pln.base_forecast_date','pln.forecast_date','src.organisation_id','locFrom.LOCATION_ID',
                                'locto.LOCATION_ID ','prd.Product_ID','pln.model_version']) }} as fct_pln_inv_storedaysku_key

from {{ref('stg_pln_mvt_depotstoredaysku')}} pln

inner join {{ ref('utl_source_organisations') }} src --need relationship validation earlier in the flow
on pln.source_db_id = src.business_organisation_number

inner join {{ ref('dim_location') }} locFrom --need relationship validation earlier in the flow
on locFrom.organisation_ID = src.organisation_ID
and locFrom.ORGANISATION_LOCATION_ID = pln.ORGANISATION_LOCATION_ID_FROM
and locFrom.location_function = 'Distribution Location'

inner join {{ ref('dim_location') }} locto --need relationship validation earlier in the flow
on locto.organisation_ID = src.organisation_ID
and locto.ORGANISATION_LOCATION_ID = pln.ORGANISATION_LOCATION_ID_to
and locto.location_function = 'Point of Sale'


inner join {{ ref('dim_product') }} prd --need relationship validation earlier in the flow
on prd.organisation_ID = src.organisation_ID
and prd.ORGANISATION_SKU = pln.ORGANISATION_SKU

        {% if is_incremental() %}
        where pln.loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}