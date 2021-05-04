{{
    config(
        materialized='table'
    )
}}

select DAY_DATE_FROM, DAY_DATE_TO, ORGANISATION_ID ORIGIN_ORGANISATION_ID, item_id,
ORGANISATION_ID SUBJECT_ORGANISATION_ID, table_reference, location_function, ACCESS_LEVEL
from
(
{{ scd_from_densified_datess('int_allfacts_visibility', 'day_date') }}
)

union all

select DAY_DATE_FROM, DAY_DATE_TO, ORIGIN_ORGANISATION_ID, item_id,
SUBJECT_ORGANISATION_ID, table_reference, location_function, ACCESS_LEVEL
from {{ ref('int_acquired_visibility') }}