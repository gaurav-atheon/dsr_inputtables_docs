select
       organisation_id,
       organisation_name,
       attributes,
       loaded_timestamp,
       is_ghost

from {{ ref('dim_organisation') }}


