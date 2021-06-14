with
atheon_orgs as --Get all orgs that have atheon as their origin
(
 select ORGANISATION_ID, BUSINESS_ORGANISATION_NUMBER, ORIGIN_ORGANISATION_NUMBER, ORIGIN_ORGANISATION_ID
 from UTL_SOURCE_ORGANISATIONS),
retailer_orgs as --Get all orgs that have a retailer as their origin
(
 select om.ORGANISATION_ID, ORIGIN_ORGANISATION_NUMBER, BUSINESS_ORGANISATION_NUMBER, BUSINESS_ORGANISATION_NAME
 from DIM_ORGANISATION_MAPPING om
 inner join DIM_ORGANISATION o
 on o.ORGANISATION_ID = om.ORIGIN_ORGANISATION_ID
 where o.ORGANISATION_TYPE = 'retailer'
)
--Find the atheon orgs that have at least one retailer child
select 
ao.BUSINESS_ORGANISATION_NUMBER organisation_code --This is the organisation ID from atheon - should be the hostname
--, ro.BUSINESS_ORGANISATION_NAME --This is the retailer supplier name
, ro.ORIGIN_ORGANISATION_NUMBER retailer_code --This is the retailer
from DIM_ORGANISATION_PARENTAGE op
inner join atheon_orgs ao
on op.PARENT_ORGANISATION_ID = ao.ORGANISATION_ID
inner join retailer_orgs ro
on ro.ORGANISATION_ID = SUBJECT_ORGANISATION_ID
group by ao.BUSINESS_ORGANISATION_NUMBER --only have one hostname regardless of the number of retailer suppliers connected
, ro.ORIGIN_ORGANISATION_NUMBER