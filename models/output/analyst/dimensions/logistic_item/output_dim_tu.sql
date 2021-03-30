select
    traded_unit_id,
    creator_organisation_id,
    loaded_timestamp
from {{ ref('dim_tu') }}