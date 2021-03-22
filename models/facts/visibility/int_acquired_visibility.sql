with
deliveries as
( --Need to make this Morrisons & Co-Op only
select day_date, organisation_id_from, product_ID
from  {{ ref('fact_act_mvt_orgdepotdaysku') }}
  where CASES_FULFILLED_IN > 0
group by day_date, organisation_id_from, product_ID
),
next_delivery as
(
select q1.*,
lead(day_date,1) OVER (PARTITION BY product_ID ORDER BY day_date) next_delivery_day
from (
select day_date, product_ID
from deliveries
group by day_date, product_ID
) q1
),
related_deliveries as
(
select t1.*, t2.next_delivery_day,
case when t3.organisation_id_from is null then 'N' else 'Y' end supplies_next_delivery,
case
  when
  t3.organisation_id_from is null --not the next supplier
  and NEXT_DELIVERY_DAY > t1.day_date +28
    then NEXT_DELIVERY_DAY --supplier remains owner until the next delivery day by another supplier
  when t3.organisation_id_from is null --not the next supplier
  and NEXT_DELIVERY_DAY <= t1.day_date +28
    then t1.day_date +28 --supplier gets ownership for 28 days
  when NEXT_DELIVERY_DAY is null then t1.day_date +365
  else NEXT_DELIVERY_DAY --supplier continues ownership until their next delivery
end valid_until
from  deliveries t1
left outer join  next_delivery t2 --next any supplier delivery
on t1.day_date = t2.day_date
and t1.product_ID = t2.product_ID
left outer join deliveries t3 --next delivery only same supplier
on t2.next_delivery_day = t3.day_date
and t1.product_ID = t3.product_ID
and t1.organisation_id_from = t3.organisation_id_from
order by t1.day_date
),
logic_test as
(
select q1.*,
lag(day_date,1) OVER (PARTITION BY product_ID, organisation_id_from ORDER BY day_date) prev_del_date,
lead(day_date,1) OVER (PARTITION BY product_ID, organisation_id_from ORDER BY day_date) sub_del_date,
case when lag(valid_until,1) OVER (PARTITION BY product_ID, organisation_id_from ORDER BY day_date) >= day_date
then 'consecutive'
else 'gap'
end start_test,
case when lead(day_date,1) OVER (PARTITION BY product_ID, organisation_id_from ORDER BY day_date) <= valid_until
then 'consecutive'
else 'gap'
end end_test
from related_deliveries q1
),
sku_ownership as
(
select organisation_id_from, product_ID, day_date start_date, end_date
from
(
  select *,
case when start_test = 'gap' and end_test = 'gap' then VALID_UNTIL
when start_test = 'gap'
then lead(valid_until,1) OVER (PARTITION BY product_ID, organisation_id_from ORDER BY day_date)
end end_date
from logic_test
  where start_test = 'gap'
  or end_test = 'gap'
)
where end_date is not null
),
min_max as
(
select min(start_date) min_date, max(end_date) max_date
from sku_ownership
),
primary_suppliers as
(
select distinct cp.product_id, s.organisation_id, cp.attributes:primary_supplier::string as primary_supplier_number --For Morrisons this maps to the suppliergroupid
from {{ ref('dim_product') }} cp
inner join {{ ref('dim_organisation_mapping') }} s
  on cp.attributes:primary_supplier::string = s.business_organisation_number
  and ORIGIN_ORGANISATION_NUMBER = 17 --How do we make this better and not hardcoded?
where cp.attributes:primary_supplier::string is not null
  ),
date_scaffold as
(
select dp.*, d.DAY_DATE
from
(
select dd.DAY_DATE
from {{ ref('dim_date') }} dd
inner join min_max mm
on dd.DAY_DATE between mm.min_date and mm.max_date
) d
cross join primary_suppliers dp
),
missing_dates as
(
select distinct ds.product_id, ds.DAY_DATE, ds.organisation_id --remove any multi-supply ownership
from date_scaffold ds
left join sku_ownership so
on ds.product_id = so.product_id
and ds.DAY_DATE between dateadd('day',1,start_date) and end_date --get an extra date, this is knocked off the end date later anyway
where so.product_id is null
),
ps_next_prev_day as
(
select md.*,
  lag(DAY_DATE) over (partition by product_id order by DAY_DATE desc) next_day,
  lead(DAY_DATE) over (partition by product_id order by DAY_DATE desc) prev_day,
  coalesce(
    datediff('day',DAY_DATE,
             lag(DAY_DATE) over (partition by product_id order by DAY_DATE desc)
            )
    , 2) days_to_next,
  coalesce(
    datediff('day',DAY_DATE,
             lead(DAY_DATE) over (partition by product_id order by DAY_DATE desc)
            )
    , -2) days_from_prev
from missing_dates md
),
ps_period as
(
  select q1.*,
  case when q1.days_from_prev <> -1 then 'start' end start_flag,
  case when q1.days_to_next <> 1 then 'end' end end_flag,
  q1.DAY_DATE start_date,
  dateadd('day',-1,lag(q1.DAY_DATE) over (partition by q1.product_id order by q1.DAY_DATE desc)) end_date -- find the end of the period and knock off a day
  from ps_next_prev_day q1
  where not (q1.days_to_next = 1 and q1.days_from_prev = -1) --only get the start and end dates of a period
),
ps_smd_dates as
(
  select q2.organisation_id, q2.product_id, q2.start_date, q2.end_date
  from ps_period q2
  where q2.START_FLAG is not null --only bring back the record for the start of the period
),
all_acquired as
(
select start_date as DAY_DATE_FROM, end_date as DAY_DATE_TO,
organisation_id_from as ORIGIN_ORGANISATION_ID, product_ID as item_id, organisation_id_from as SUBJECT_ORGANISATION_ID,
NULL as table_reference, NULL as location_function, '300 - Acquired' as ACCESS_LEVEL
from sku_ownership
union all
select start_date as DAY_DATE_FROM, end_date as DAY_DATE_TO,
organisation_id as ORIGIN_ORGANISATION_ID, product_ID as item_id, organisation_id as SUBJECT_ORGANISATION_ID,
NULL as table_reference, NULL as location_function, '300 - Acquired' as ACCESS_LEVEL
from ps_smd_dates
)
select aa.DAY_DATE_FROM, aa.DAY_DATE_TO, ORIGIN_ORGANISATION_ID, item_id, SUBJECT_ORGANISATION_ID,
af.table_reference, aa.location_function, aa.ACCESS_LEVEL
from all_acquired aa
cross join {{ ref('int_acquired_facts') }} af
