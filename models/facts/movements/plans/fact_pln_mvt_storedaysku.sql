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
    src.organisation_id as organisation_id,
    org.organisation_id as subject_organisation_id,
    loc.location_id as subject_location_id,
    prd.product_id,
    pln.units_required,
    pln.units_required_upper,
    pln.units_required_lower,
    pln.forecast_components,
    pln.created_at,
    pln.model_version,
    pln.origin_file,
    pln.loaded_timestamp,
    {{ dbt_utils.surrogate_key(['pln.day_date','pln.forecast_date','src.organisation_id','loc.location_id'
                                ,'prd.product_id','pln.model_version']) }} as unique_key

from {{ref('stg_pln_mvt_storedaysku')}} pln

inner join {{ ref('utl_source_organisations') }} src --need relationship validation earlier in the flow
on pln.source_db_id = src.business_organisation_number

inner join {{ ref('utl_source_organisations') }} org --need relationship validation earlier in the flow
on pln.subject_business_organisation_number = org.business_organisation_number

inner join {{ ref('dim_product') }} prd --need relationship validation earlier in the flow
on prd.organisation_id = src.organisation_id
and prd.organisation_sku = pln.organisation_sku

inner join {{ ref('dim_location') }} loc --need relationship validation earlier in the flow
on loc.organisation_id = src.organisation_id
and loc.organisation_location_id = pln.subject_organisation_location_id_to
and loc.location_function = 'point of sale'

        {% if is_incremental() %}
        where pln.loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}