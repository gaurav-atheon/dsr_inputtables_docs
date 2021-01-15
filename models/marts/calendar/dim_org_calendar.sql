select
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_ID,
    DAY_DATE,
    WEEK_ID,
    YEAR_ID
from {{ ref('stg_calendar') }}