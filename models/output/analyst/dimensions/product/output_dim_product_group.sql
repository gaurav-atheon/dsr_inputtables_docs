select
    product_id,
    creator_organisation_id,
    consumer_unit_id,
    group_name,
    group_value,
    attributes,
    loaded_timestamp,
    runstartedtime
from {{ ref('dim_product_group') }}