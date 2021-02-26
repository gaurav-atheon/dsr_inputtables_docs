{{
    config(
        materialized='incremental',
        unique_key='fct_pln_inv_orgdepotdaysku_key',
        cluster_by=['loaded_timestamp']
    )
}}

select
    pln.day_date,
    pln.forecast_date,
    src.organisation_id as creator_organisation_id,
    orgto.organisation_id as subject_organisation_id_to,
    orgfrom.organisation_id as subject_organisation_id_from,
    loc.location_id as subject_location_id,
    prd.product_id as subject_product_id,
    pln.units_required,
    pln.units_required_upper,
    pln.units_required_lower,
    pln.cases_required,
    pln.cases_required_upper,
    pln.cases_required_lower,
    pln.forecast_components,
    pln.created_at,
    pln.model_version,
    pln.origin_file,
    pln.loaded_timestamp,
    {{ dbt_utils.surrogate_key(['pln.day_date','pln.forecast_date','src.organisation_id','orgto.organisation_id','orgfrom.organisation_id',
                                'loc.location_id ','prd.product_id','pln.model_version']) }} as fct_pln_inv_orgdepotdaysku_key

from {{ref('stg_pln_mvt_orgdepotdaysku')}} pln

inner join {{ ref('utl_source_organisations') }} src --need relationship validation earlier in the flow
on pln.source_db_id = src.business_organisation_number

inner join {{ ref('dim_organisation_mapping') }} orgto --need relationship validation earlier in the flow
on pln.subject_business_organisation_number_to = orgto.business_organisation_number
and orgto.origin_organisation_id = src.organisation_id

inner join {{ ref('dim_organisation_mapping') }} orgfrom --need relationship validation earlier in the flow
on pln.subject_business_organisation_number_from = orgfrom.business_organisation_number
and orgfrom.origin_organisation_id = src.organisation_id

inner join {{ ref('dim_location') }} loc --need relationship validation earlier in the flow
on loc.organisation_id = src.organisation_id
and loc.organisation_location_id = pln.subject_organisation_location_id_to
and loc.location_function = 'distribution location'


inner join {{ ref('dim_product') }} prd --need relationship validation earlier in the flow
on prd.organisation_id = orgto.organisation_id
and prd.organisation_sku = pln.subject_organisation_sku

        {% if is_incremental() %}
        where pln.loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}