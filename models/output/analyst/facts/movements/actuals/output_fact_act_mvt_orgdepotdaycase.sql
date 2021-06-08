select
    day_date,
    organisation_id_to,
    organisation_id_from,
    location_id,
    logisticitem_id,
    cases_ordered_in,
    cases_fulfilled_in,
    loaded_timestamp,
    unique_key,
    runstartedtime

from {{ ref('fact_act_mvt_orgdepotdaycase') }}