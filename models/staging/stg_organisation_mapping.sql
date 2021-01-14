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
    {{ dbt_utils.surrogate_key(['a.origin_organisation_number','a.business_organisation_number']) }} as organisation_id,
    a.business_organisation_number,
    a.business_organisation_name,
    a.origin_organisation_number,
    {{ dbt_utils.surrogate_key(['b.origin_organisation_number','a.origin_organisation_number']) }} as origin_organisation_id,
    a.loaded_timestamp,
    row_number() over (partition by a.origin_organisation_number, a.business_organisation_number order by a.loaded_timestamp desc) rank
from all_data a
left outer join all_data b
on a.origin_organisation_number = b.business_organisation_number
and b.origin_organisation_number = ( select business_organisation_number from {{ ref('utl_master_organisation') }} )
)

select *
from ranked_data
where rank = 1
