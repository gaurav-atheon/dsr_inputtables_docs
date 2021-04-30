SELECT location_id,
       parent_location_id,
       Level,
       location_group_id
from {{ ref('utl_location_hierarchy') }}