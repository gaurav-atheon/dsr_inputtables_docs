select
o.location_id,
o.organisation_id,
o.organisation_location_id,
o.geographic_location,
o.location_function,
o.attributes,
o.loaded_timestamp,
o.is_ghost,
p.parent_location_id,
o.runstartedtime
from {{ ref('dim_location') }} o
left outer join {{ ref('dim_location_parentage') }} p
on o.location_id = p.subject_location_id
--and p.creator_organisation_id = (select organisation_id from {{ ref('utl_master_organisation') }})