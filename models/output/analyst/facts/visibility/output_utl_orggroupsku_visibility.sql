  SELECT table_reference,
         item_id,
         DAY_DATE,
         organisation_group_id,
         min_access_level
  FROM   {{ ref('utl_orggroupsku_visibility') }}
