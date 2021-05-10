select
    product_id,
    creator_organisation_id,
    consumer_unit_id,
    group_name,
    group_value,
    loaded_timestamp
from {{ ref('dim_product_group') }}