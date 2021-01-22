{{
    config(
        materialized='table'
    )
}}

with
orders as
(
select *
from {{ ref('int_allfacts_visibility') }}
),

next_order as
(
select o.*,
  lead(o.day_date) over (partition by Organisation_ID, Product_ID, table_reference order by o.day_date asc) next_order_date,
  datediff(day, o.day_date, lead(o.day_date) over (partition by Organisation_ID, Product_ID, table_reference order by o.day_date desc)) diff_days_desc,
  datediff(day, o.day_date, lead(o.day_date) over (partition by Organisation_ID, Product_ID, table_reference order by o.day_date asc)) diff_days_asc,
  case
    when datediff(day, o.day_date, lead(o.day_date) over (partition by Organisation_ID, Product_ID, table_reference order by o.day_date desc)) < -1
        then 'Start'
    when datediff(day, o.day_date, lead(o.day_date) over (partition by Organisation_ID, Product_ID, table_reference order by o.day_date desc)) is null
        then 'Start'
    else 'Continuous'
  end marker_start,
  case
    when datediff(day, o.day_date, lead(o.day_date) over (partition by Organisation_ID, Product_ID, table_reference order by o.day_date asc)) > 1
        then 'End'
    when datediff(day, o.day_date, lead(o.day_date) over (partition by Organisation_ID, Product_ID, table_reference order by o.day_date asc)) is null
        then 'End'
    else 'Continuous'
  end marker_end
  from orders o
),

end_points as
(
select ord.*, lead(ord.day_date) over (partition by Organisation_ID, Product_ID, table_reference order by ord.day_date asc) end_of_run
from next_order ord
--Knock out the intervening dates
where MARKER_START <> 'Continuous'
or MARKER_END <> 'Continuous'
),

scd as (
select q1.DAY_DATE DAY_DATE_FROM,
  case
  when MARKER_END = 'End' then DAY_DATE
  when END_OF_RUN is null then DAY_DATE
  else END_OF_RUN end DAY_DATE_TO,
ORGANISATION_ID ORIGIN_ORGANISATION_ID, Product_ID, ORGANISATION_ID SUBJECT_ORGANISATION_ID, table_reference, ACCESS_LEVEL
from end_points q1
where MARKER_START = 'Start' --Only keep the start of a run
)

select *
from scd