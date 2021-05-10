select
       organisation_id,
       organisation_name,
       attributes,
       organisation_type,
       loaded_timestamp,
       is_ghost

from {{ ref('dim_organisation') }}


