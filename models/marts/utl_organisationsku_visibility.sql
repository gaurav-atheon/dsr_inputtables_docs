select *
from {{ ref('int_storedaysku_orders_visibility') }} ord


  day_date_from DATE,
  day_date_to DATE,
  origin_organisation_id int,
  Organisation_SKU varchar(100),
  subject_organisation_id int,
  table_reference varchar,
  access_level varchar
