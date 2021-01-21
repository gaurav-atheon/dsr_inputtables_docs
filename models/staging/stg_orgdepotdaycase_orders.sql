with
ranked_data as
(
select
    day_date,
    source_db_id,
    organisation_location_id,
    BUSINESS_ORGANISATION_NUMBER_FROM,
    ORGANISATION_CASE,
    CASES_ORDERED_IN,
    CASES_MATCHED_IN,
    loaded_timestamp,
    row_number() over (partition by day_date, source_db_id, organisation_location_id, ORGANISATION_CASE order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_orgdepotdaycase_orders') }}
)

select *
from ranked_data
where rank = 1