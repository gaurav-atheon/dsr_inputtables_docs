select
o.organisation_id,
o.organisation_name,
o.attributes,
o.loaded_timestamp,
o.is_ghost,
p.parent_organisation_id
from {{ ref('dim_organisation') }} o
left outer join {{ ref('dim_organisation_parentage') }} p
on o.organisation_id = p.subject_organisation_id
and p.creator_organisation_id = (select organisation_id from {{ ref('utl_master_organisation') }})