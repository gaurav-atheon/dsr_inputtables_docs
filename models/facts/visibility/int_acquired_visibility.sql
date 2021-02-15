
--choose which source DB to do this with
--join with a list "table" of the facts that need to have acquired visibility
with
deliveries as
(
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
ownership as
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
from
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
from ownership q1
  )
  where start_test = 'gap'
  or end_test = 'gap'
order by day_date, organisation_id_from
)
where end_date is not null
)
--,
--primary_suppliers as --Need to do this with some attributes from morrisons skus, remove for now
--(
--select distinct R_SKU R_MIN, R_SUPPLIERGROUP, R_SUPPLIERGROUPID
--from DIM_TBL_CONSUMERPRODUCT cp
--inner join DIM_TBL_SUPPLIER s
--  on cp.R_PRIMARY_SUPPLIER = s.R_SUPPLIERGROUPID
--where cp.r_primary_supplier is not null
--  ),
--date_scaffold as
--(
--select dp.*, d.D_FULLDATE
--from utl_tbl_date d
--cross join primary_suppliers dp
--),
--missing_dates as
--(
--select distinct ds.R_MIN, ds.D_FULLDATE, ds.R_SUPPLIERGROUP, ds.R_SUPPLIERGROUPID --remove any multi-supply ownership
--from date_scaffold ds
--left join sku_ownership so
--on ds.R_MIN = so.R_MIN
--and ds.D_FULLDATE between dateadd('day',1,start_date) and end_date --get an extra date, this is knocked off the end date later anyway
--where so.R_MIN is null
--),
--smd_dates as
--(
--  select q2.R_SUPPLIERGROUP, q2.R_SUPPLIERGROUPID, q2.R_MIN, q2.start_date, q2.end_date
--  from
--  (
--  select q1.*,
--  case when q1.days_from_prev <> -1 then 'start' end start_flag,
--  case when q1.days_to_next <> 1 then 'end' end end_flag,
--  q1.D_FULLDATE start_date,
--  dateadd('day',-1,lag(q1.D_FULLDATE) over (partition by q1.R_MIN order by q1.D_FULLDATE desc)) end_date -- find the end of the period and knock off a day
--  from
--  (
--select md.*,
--  lag(D_FULLDATE) over (partition by R_MIN order by D_FULLDATE desc) next_day,
--  lead(D_FULLDATE) over (partition by R_MIN order by D_FULLDATE desc) prev_day,
--  coalesce(
--    datediff('day',D_FULLDATE,
--             lag(D_FULLDATE) over (partition by R_MIN order by D_FULLDATE desc)
--            )
--    , 2) days_to_next,
--  coalesce(
--    datediff('day',D_FULLDATE,
--             lead(D_FULLDATE) over (partition by R_MIN order by D_FULLDATE desc)
--            )
--    , -2) days_from_prev
--from missing_dates md
--    ) q1
--  where not (q1.days_to_next = 1 and q1.days_from_prev = -1) --only get the start and end dates of a period
--    ) q2
--  where q2.START_FLAG is not null --only bring back the record for the start of the period
--)
select start_date as DAY_DATE_FROM, end_date as DAY_DATE_TO,
organisation_id_from as ORIGIN_ORGANISATION_ID, product_ID as item_id, organisation_id_from as SUBJECT_ORGANISATION_ID,
NULL as table_reference, NULL as location_function, '300 - Acquired' as ACCESS_LEVEL
from sku_ownership
--union all
--select *
--from smd_dates
