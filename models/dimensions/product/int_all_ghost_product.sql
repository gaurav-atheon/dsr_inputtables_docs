{{
    config(
        materialized='table'
    )
}}
select  all_fact_data.Product_ID, all_fact_data.organisation_id,all_fact_data.ORGANISATION_SKU,all_fact_data.DESCRIPTION,all_fact_data.INDIVIDUAL_UNITS,
        all_fact_data.NET_QUANTITY,all_fact_data.BASE_UNIT,all_fact_data.BRAND,all_fact_data.GTIN, max(loaded_timestamp) as loaded_timestamp, all_fact_data.is_ghost
from (

    {{ ghost_product_entries(stg_of_fact_table='stg_act_inv_locationdaysku') }}

union

    {{ ghost_product_entries(stg_of_fact_table='stg_pln_inv_storedaysku') }}

union

    {{ ghost_product_entries(stg_of_fact_table='stg_act_mvt_depotstoredaysku')}}

union

     {{ ghost_product_entries(stg_of_fact_table='stg_pln_mvt_depotstoredaysku')}}

union

     {{ ghost_product_entries(stg_of_fact_table='stg_act_mvt_storedaysku')}}

) all_fact_data
group by all_fact_data.Product_ID, all_fact_data.organisation_id,all_fact_data.ORGANISATION_SKU,all_fact_data.DESCRIPTION,
         all_fact_data.INDIVIDUAL_UNITS,all_fact_data.NET_QUANTITY,all_fact_data.BASE_UNIT,all_fact_data.BRAND,all_fact_data.GTIN, all_fact_data.is_ghost