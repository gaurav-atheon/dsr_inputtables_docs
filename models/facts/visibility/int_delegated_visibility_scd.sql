{{
    config(
        materialized='table'
    )
}}

select DAY_DATE_FROM, DAY_DATE_TO, delegatee_organisation_id ORIGIN_ORGANISATION_ID, product_type, item_id,
delegatee_organisation_id SUBJECT_ORGANISATION_ID, table_reference, location_function, ACCESS_LEVEL
from
(
{{ scd_from_densified_dates('int_delegated_visibility', 'day_date') }}
)
