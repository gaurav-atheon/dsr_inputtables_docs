select
    day_date,
    year,
    month,
    month_name,
    day_of_month,
    day_of_week,
    week_of_year,
    day_of_year,
    loaded_timestamp
from {{ ref('dim_date') }}
