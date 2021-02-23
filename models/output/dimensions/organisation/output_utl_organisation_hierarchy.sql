SELECT organisation_id,
       parent_organisation_id,
       Level,
       organisation_group_id
from {{ ref('utl_organisation_hierarchy') }}