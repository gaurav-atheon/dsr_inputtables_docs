select
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_id,
    day_date,
    week_id,
    year_id
from {{ ref('stg_calendar') }}