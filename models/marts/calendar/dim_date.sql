select
    DAY_DATE,
    YEAR,
    MONTH,
    MONTH_NAME,
    DAY_OF_MONTH,
    DAY_OF_WEEK,
    WEEK_OF_YEAR,
    DAY_OF_YEAR
from {{ ref('stg_date') }}