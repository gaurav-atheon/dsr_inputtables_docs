select
    day_date,
    organisation_id,
    organisation_location_id_from,
    organisation_location_id_to,
    product_id,
    units_ordered,
    units_fulfilled,
    loaded_timestamp,
    unique_key,
    runstartedtime

from {{ ref('fact_act_mvt_depotstoredaysku') }}