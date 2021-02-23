select
    consumer_unit_id,
    creator_organisation_id,
    loaded_timestamp
from {{ ref('dim_cu') }}
