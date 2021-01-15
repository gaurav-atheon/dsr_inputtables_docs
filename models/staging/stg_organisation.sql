with
ranked_data as
(
select
    origin_organisation_number,
    business_organisation_number,
    type,
    address,
    parent_origin_organisation_number,
    parent_organisation_number,
    loaded_timestamp,
    row_number() over (partition by origin_organisation_number,business_organisation_number order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_organisation') }}
)

select *
from ranked_data
where rank = 1