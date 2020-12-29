with
ranked_data as
(
select
    organisation_id,
    type,
    address,
    parent_organisation_id,
    loaded_timestamp,
    row_number() over (partition by organisation_id order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_organisation') }}
)

select *
from ranked_data
where rank = 1