select
    day_date,
    forecast_date,
    organisation_id,
    organisation_location_id_from,
    organisation_location_id_to,
    product_id,
    units_required,
    units_required_upper,
    units_required_lower,
    forecast_components,
    created_at,
    model_version,
    origin_file,
    loaded_timestamp,
    fct_pln_inv_storedaysku_key

from {{ref('fact_pln_mvt_depotstoredaysku')}}