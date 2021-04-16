select
    promotion_id,
    creator_organisation_id,
    subject_organisation_id,
    promotion_number,
    start_date,
    end_date,
    product_id,
    attributes,
    created_timestamp,
    loaded_timestamp,
    is_ghost
from {{ ref('dim_promotion') }}
