with
ranked_data as
(
select
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','ORGANISATION_LOCATION_ID']) }} as LOCATION_ID,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_ID,
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