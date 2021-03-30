select
    logisticitem_id,
    creator_organisation_id,
    traded_unit_id,
    loaded_timestamp
from {{ ref('dim_logisticitem_mapping') }}
