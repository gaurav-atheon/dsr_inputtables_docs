select
    traded_unit_id,
    creator_organisation_id,
    loaded_timestamp,
    runstartedtime
from {{ ref('dim_tu') }}