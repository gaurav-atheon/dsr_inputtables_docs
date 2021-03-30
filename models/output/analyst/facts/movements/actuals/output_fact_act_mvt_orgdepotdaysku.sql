select
    day_date,
    organisation_id_to,
    organisation_id_from,
    LOCATION_ID,
    product_ID,
    CASES_ORDERED_IN,
    CASES_FULFILLED_IN
from  {{ ref('fact_act_mvt_orgdepotdaysku') }}
