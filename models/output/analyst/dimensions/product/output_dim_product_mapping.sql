select
    product_id,
    creator_organisation_id,
    consumer_unit_id,
    loaded_timestamp
from {{ ref('dim_product_mapping') }}