{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='unique_key',
        cluster_by=['loaded_timestamp']
    )
}}

select
    pln.day_date,
    pln.forecast_date,
    src.organisation_id as organisation_id,
    org.organisation_id as subject_organisation_id,
    prd.product_id,
    pln.units_required,
    pln.units_required_upper,
    pln.units_required_lower,
    pln.forecast_components,
    pln.created_at,
    pln.model_version,
    pln.origin_file,
    pln.loaded_timestamp,
    '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/London")) }}'  as runstartedtime,
    {{ dbt_utils.surrogate_key(['pln.day_date','pln.forecast_date','src.organisation_id'
                                ,'prd.product_id','pln.model_version']) }} as unique_key

from {{ref('stg_pln_mvt_daysku')}} pln

inner join {{ ref('utl_source_organisations') }} src --need relationship validation earlier in the flow
on pln.source_db_id = src.business_organisation_number

inner join {{ ref('dim_organisation_mapping') }} org --need relationship validation earlier in the flow
on pln.subject_business_organisation_number = org.business_organisation_number
and org.origin_organisation_id = src.organisation_id

inner join {{ ref('dim_product') }} prd --need relationship validation earlier in the flow
on prd.organisation_id = src.organisation_id
and prd.organisation_sku = pln.organisation_sku

        {% if is_incremental() %}
        where pln.loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}