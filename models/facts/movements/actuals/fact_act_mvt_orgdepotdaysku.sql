select
    f.day_date,
    f.organisation_id_to,
    f.organisation_id_from,
    f.LOCATION_ID,
    coalesce(li.product_ID, p.product_id) as product_id,
    li.case_size,
    min(f.loaded_timestamp) as loaded_timestamp,
    min(f.runstartedtime) as runstartedtime,
    sum(f.CASES_ORDERED_IN) as CASES_ORDERED_IN,
    sum(f.CASES_FULFILLED_IN) as CASES_FULFILLED_IN
from  {{ ref('fact_act_mvt_orgdepotdaycase') }} f
    inner join {{ ref('dim_logisticitem') }} li
    on f.logisticitem_ID = li.logisticitem_ID

    left join {{ ref('dim_product') }} p
    on li.organisation_case = p.organisation_sku
    and p.organisation_id = '{{is_specific_retailer("morrisons")}}'

group by
    f.day_date,
    f.organisation_id_to,
    f.organisation_id_from,
    f.LOCATION_ID,
    coalesce(li.product_ID, p.product_id),
    li.case_size