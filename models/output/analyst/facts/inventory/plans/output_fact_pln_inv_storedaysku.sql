select
    day_date,
    organisation_id,
    location_id,
    product_id,
    ranged,
    loaded_timestamp,
    unique_key

from {{ref('fact_pln_inv_storedaysku')}} ord
