with
ranked_data as
(
select
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_ID,
    DAY_DATE,
    WEEK_ID,
    YEAR_ID,
    row_number() over (partition by origin_organisation_number,business_organisation_number,DAY_DATE order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_calendar') }}
)

select *
from ranked_data
where rank = 1