select
    day_date,
    organisation_id,
    location_id,
    product_id,
    ranged,
    loaded_timestamp,
    fct_act_mvt_storedaysku_key

from {{ref('fact_pln_inv_storedaysku')}} ord
