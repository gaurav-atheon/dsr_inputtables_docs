select
    day_date,
    organisation_id_to,
    organisation_id_from,
    location_id,
    product_id,
    case_size,
    cases_ordered_in,
    cases_fulfilled_in,
    loaded_timestamp,
    runstartedtime
from  {{ ref('fact_act_mvt_orgdepotdaysku') }}
