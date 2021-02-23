select
    day_date,
    organisation_id_to,
    organisation_id_from,
    location_id,
    logisticitem_id,
    cases_ordered_in,
    cases_fulfilled_in,
    loaded_timestamp,
    fct_act_mvt_orgdepotdaycase_key

from {{ ref('fact_act_mvt_orgdepotdaycase') }}