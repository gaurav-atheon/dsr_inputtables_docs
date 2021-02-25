select
    day_date,
    forecast_date,
    creator_organisation_id,
    subject_organisation_id_to,
    subject_organisation_id_from,
    subject_location_id,
    subject_product_id,
    units_required,
    units_required_upper,
    units_required_lower,
    case_required,
    case_required_upper,
    case_required_lower,
    forecast_components,
    created_at,
    model_version,
    origin_file,
    loaded_timestamp,
    fct_pln_inv_locationdaysku_key

from {{ref('fact_pln_mvt_orgdepotdaysku')}}