{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='logisticitem_id',
        cluster_by=['runstartedtime']
    )
}}
select  all_fact_data.logisticitem_id,all_fact_data.organisation_id,all_fact_data.product_id,all_fact_data.organisation_sku,all_fact_data.organisation_case,
        all_fact_data.case_size,all_fact_data.gtin,max(all_fact_data.loaded_timestamp) as loaded_timestamp,all_fact_data.is_ghost,max(runstartedtime)  as runstartedtime
from (

    {{ ghost_logisticitem_entries(stg_of_fact_table='stg_act_inv_locationdaycase') }}

union

    {{ ghost_logisticitem_entries(stg_of_fact_table='stg_act_mvt_orgdepotdaycase') }}

union

    {{ ghost_logisticitem_entries(stg_of_fact_table='stg_act_mvt_orgdepotdayordercase') }}


) all_fact_data
group by all_fact_data.logisticitem_id,all_fact_data.organisation_id,all_fact_data.product_id,all_fact_data.organisation_sku,
         all_fact_data.organisation_case,all_fact_data.case_size,all_fact_data.gtin,all_fact_data.is_ghost