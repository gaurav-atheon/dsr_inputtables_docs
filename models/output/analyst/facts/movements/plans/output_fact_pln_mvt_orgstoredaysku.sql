select
    day_date,
    forecast_date,
    organisation_id,
    subject_organisation_id,
    subject_location_id,
    product_id,
    units_required,
    units_required_upper,
    units_required_lower,
    forecast_components,
    created_at,
    model_version,
    origin_file,
    loaded_timestamp,
    unique_key,
    runstartedtime

from {{ref('fact_pln_mvt_orgstoredaysku')}}