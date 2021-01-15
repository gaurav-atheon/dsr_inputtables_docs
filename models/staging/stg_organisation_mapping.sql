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
    a.business_organisation_number,
    a.business_organisation_name,
    a.origin_organisation_number,
    a.loaded_timestamp,
    row_number() over (partition by a.origin_organisation_number, a.business_organisation_number order by a.loaded_timestamp desc) rank
from all_data a
)

select *
from ranked_data
where rank = 1