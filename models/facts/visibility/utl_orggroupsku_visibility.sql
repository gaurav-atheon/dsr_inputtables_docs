 {{
    config(
        materialized='table'
    )
}}

with all_scd as
(
--Find all the scd's
select  
v.DAY_DATE_FROM,
v.DAY_DATE_TO,
v.product_type,
v.item_id,
o.organisation_group_id,
v.table_reference,
v.location_function
  FROM {{ ref('utl_organisationsku_visibility') }} v

--Join in the org group 
        inner join {{ ref('utl_organisation_hierarchy') }} o
          ON v.SUBJECT_ORGANISATION_ID = o.organisation_id
)

select *
from all_scd
match_recognize(
                       partition BY table_reference, product_type, item_id, organisation_group_id, location_function
ORDER BY DAY_DATE_FROM, DAY_DATE_TO MEASURES FIRST(DAY_DATE_FROM) DAY_DATE_FROM, max(DAY_DATE_TO) DAY_DATE_TO
PATTERN(a* b)
DEFINE a AS MAX(DATEADD(day,1,DAY_DATE_TO)) OVER() >= NEXT(DAY_DATE_FROM)
      ) mr