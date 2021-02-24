select
    organisation_id,
    day_date,
    week_id,
    year_id
from {{ ref('dim_org_calendar') }}