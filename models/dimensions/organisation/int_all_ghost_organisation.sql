{{
    config(
        materialized='table'
    )
}}
select organisation_id,organisation_name,attributes,max(loaded_timestamp) as loaded_timestamp,is_ghost
from (

    {{ ghost_organisation_entries(stg_of_fact_table='stg_act_mvt_orgdepotdaycase') }}

) group by
    organisation_id,organisation_name,attributes,is_ghost