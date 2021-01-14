with
ranked_data as
(
select
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_id,
    type,
    address,
    {{ dbt_utils.surrogate_key(['parent_origin_organisation_number','parent_organisation_number']) }} as parent_organisation_id,
    loaded_timestamp,
    row_number() over (partition by origin_organisation_number, business_organisation_number order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_organisation') }}
)

select *
from ranked_data
where rank = 1