{{
    config(
        materialized='table'
    )
}}
select  all_fact_data.product_id, all_fact_data.organisation_id,all_fact_data.organisation_sku,all_fact_data.description,all_fact_data.individual_units,all_fact_data.net_quantity,
        all_fact_data.base_unit,all_fact_data.brand,all_fact_data.gtin, max(loaded_timestamp) as loaded_timestamp,all_fact_data.attributes, all_fact_data.is_ghost
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
group by all_fact_data.product_id, all_fact_data.organisation_id,all_fact_data.organisation_sku,all_fact_data.description,all_fact_data.individual_units,
         all_fact_data.net_quantity,all_fact_data.base_unit,all_fact_data.brand,all_fact_data.gtin, all_fact_data.attributes,all_fact_data.is_ghost