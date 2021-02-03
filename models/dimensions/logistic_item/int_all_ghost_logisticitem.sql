{{
    config(
        materialized='table'
    )
}}
select  all_fact_data.logisticitem_ID,all_fact_data.organisation_ID,all_fact_data.product_ID,all_fact_data.ORGANISATION_SKU,all_fact_data.ORGANISATION_CASE,
        all_fact_data.CASE_SIZE,all_fact_data.GTIN,max(all_fact_data.loaded_timestamp) as loaded_timestamp,all_fact_data.is_ghost
from (

    {{ ghost_logisticitem_entries(stg_of_fact_table='stg_act_inv_locationdaycase') }}

union

    {{ ghost_logisticitem_entries(stg_of_fact_table='stg_act_mvt_orgdepotdaycase') }}


) all_fact_data
group by all_fact_data.logisticitem_ID,all_fact_data.organisation_ID,all_fact_data.product_ID,all_fact_data.ORGANISATION_SKU,
         all_fact_data.ORGANISATION_CASE,all_fact_data.CASE_SIZE,all_fact_data.GTIN,all_fact_data.is_ghost