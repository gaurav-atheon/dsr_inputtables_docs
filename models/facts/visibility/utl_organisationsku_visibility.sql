{{
    config(
        materialized='table'
    )
}}


select DAY_DATE_FROM, DAY_DATE_TO, ORIGIN_ORGANISATION_ID, product_type, item_id,
SUBJECT_ORGANISATION_ID, table_reference, location_function, ACCESS_LEVEL
from {{ ref('int_organisationsku_visibility') }}

union all

select DAY_DATE_FROM, DAY_DATE_TO, ORIGIN_ORGANISATION_ID, product_type, item_id,
SUBJECT_ORGANISATION_ID, table_reference, location_function, ACCESS_LEVEL
from {{ ref('int_delegated_visibility_scd') }}