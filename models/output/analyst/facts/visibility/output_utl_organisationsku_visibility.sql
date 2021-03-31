select
       DAY_DATE_FROM,
       DAY_DATE_TO,
       ORIGIN_ORGANISATION_ID,
       item_id,
       SUBJECT_ORGANISATION_ID,
       table_reference,
       location_function,
       ACCESS_LEVEL
from {{ ref('utl_organisationsku_visibility') }}