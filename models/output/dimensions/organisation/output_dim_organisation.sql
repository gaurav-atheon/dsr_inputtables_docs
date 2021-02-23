select
       organisation_id,
       organisation_name,
       type,
       address,
       loaded_timestamp,
       is_ghost

from {{ ref('dim_organisation') }}


