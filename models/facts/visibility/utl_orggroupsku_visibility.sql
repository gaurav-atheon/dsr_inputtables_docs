 {{
    config(
        materialized='table'
    )
}}

select DAY_DATE_FROM, DAY_DATE_TO, product_type, item_id,
organisation_group_id, table_reference, location_function
from
(
{{ scd_from_densified_dates('int_orggroupsku_visibility', 'DAY_DATE') }}
)