select
    organisation_ID,
    DAY_DATE,
    WEEK_ID,
    YEAR_ID
from {{ ref('stg_calendar') }}