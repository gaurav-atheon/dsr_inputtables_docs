with
ranked_data as
(
select
    ORGANISATION_LOCATION_ID,
    origin_organisation_number,
    business_organisation_number,
    GEOGRAPHIC_LOCATION,
    LOCATION_FUNCTION,
    row_number() over (partition by origin_organisation_number,business_organisation_number,ORGANISATION_LOCATION_ID order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_location') }}
)

select *
from ranked_data
where rank = 1