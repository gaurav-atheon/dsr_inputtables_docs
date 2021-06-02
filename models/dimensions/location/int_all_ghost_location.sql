{{
    config(
        materialized='incremental',
        unique_key='location_id',
        cluster_by=['runstartedtime']
    )
}}
select location_ID,
    organisation_ID,
    organisation_location_id,
    geographic_location,
    attributes,
    location_function,max(loaded_timestamp) as loaded_timestamp, is_ghost,max(runstartedtime)  as runstartedtime from (
{{ ghost_location_entries(stg_of_fact_table='stg_act_inv_locationdaycase') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_act_inv_locationdaysku') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_pln_inv_storedaysku',fact_location_function='point of sale') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_act_mvt_depotstoredaysku',location_col_name='ORGANISATION_LOCATION_ID_FROM',fact_location_function='distribution location') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_act_mvt_depotstoredaysku',location_col_name='ORGANISATION_LOCATION_ID_TO',fact_location_function='point of sale') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_act_mvt_orgdepotdaycase',fact_location_function='distribution location') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_act_mvt_orgdepotdaycase',fact_location_function='point of sale') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_act_mvt_storedaysku',fact_location_function='point of sale') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_pln_mvt_depotstoredaysku',location_col_name='ORGANISATION_LOCATION_ID_FROM',fact_location_function='distribution location') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_pln_mvt_depotstoredaysku',location_col_name='ORGANISATION_LOCATION_ID_TO',fact_location_function='point of sale') }}

union

{{ ghost_location_entries(stg_of_fact_table='stg_pln_mvt_orgdepotdaysku',location_col_name='SUBJECT_ORGANISATION_LOCATION_ID_TO',fact_location_function='distribution location') }}

) group by  location_ID, organisation_id, organisation_location_id, geographic_location,location_function,attributes, is_ghost