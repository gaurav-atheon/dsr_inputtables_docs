{{
    config(
        materialized='table'
    )
}}
select location_ID,
    organisation_ID,
    ORGANISATION_LOCATION_ID,
    GEOGRAPHIC_LOCATION,
    LOCATION_FUNCTION,max(loaded_timestamp) as loaded_timestamp, is_ghost from (
{{ ghost_location_entries(stg_of_fact_table='stg_act_inv_locationdaycase') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_act_inv_locationdaysku') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_pln_inv_storedaysku',fact_location_function='Point of Sale') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_act_mvt_depotstoredaysku',location_col_name='ORGANISATION_LOCATION_ID_FROM',fact_location_function='Distribution Location') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_act_mvt_depotstoredaysku',location_col_name='ORGANISATION_LOCATION_ID_TO',fact_location_function='Point of Sale') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_act_mvt_orgdepotdaycase',fact_location_function='Distribution Location') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_act_mvt_orgdepotdaycase',fact_location_function='Point of Sale') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_act_mvt_storedaysku',fact_location_function='Point of Sale') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_pln_mvt_depotstoredaysku',location_col_name='ORGANISATION_LOCATION_ID_FROM',fact_location_function='Distribution Location') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_pln_mvt_depotstoredaysku',location_col_name='ORGANISATION_LOCATION_ID_TO',fact_location_function='Point of Sale') }}
) group by     location_ID,
    organisation_ID,
    ORGANISATION_LOCATION_ID,
    GEOGRAPHIC_LOCATION,
    LOCATION_FUNCTION,
           is_ghost