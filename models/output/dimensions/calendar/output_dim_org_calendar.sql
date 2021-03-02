select
    organisation_id,
    day_date,
    week_id,
    year_id,
    weekcommencing,
    weekending,
    dayofweeknumber,
    day,
    loaded_timestamp
from {{ ref('dim_org_calendar') }}