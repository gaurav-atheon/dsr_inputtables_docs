with
all_data as
(
{{ dbt_utils.union_relations(
    relations=[ref('master_organisation'), source('dsr_input', 'input_organisation_mapping')],
) }}
),

ranked_data as
(
select
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_id,
    origin_organisation_number,
    origin_organisation_name,
    business_organisation_number,
    business_organisation_name,
    loaded_timestamp,
    row_number() over (partition by origin_organisation_number, business_organisation_number order by loaded_timestamp desc) rank
from all_data
)

select *
from ranked_data
where rank = 1