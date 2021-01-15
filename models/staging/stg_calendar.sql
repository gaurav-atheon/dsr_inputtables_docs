with
ranked_data as
(
select
    DAY_DATE,
    WEEK_ID,
    YEAR_ID,
    origin_organisation_number,
    business_organisation_number,
    row_number() over (partition by origin_organisation_number,business_organisation_number,DAY_DATE order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_calendar') }}
)

select *
from ranked_data
where rank = 1