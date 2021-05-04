  SELECT table_reference,
         item_id,
         DAY_DATE_FROM,
         DAY_DATE_TO,
         organisation_group_id,
         location_function
  FROM   {{ ref('utl_orggroupsku_visibility') }}
