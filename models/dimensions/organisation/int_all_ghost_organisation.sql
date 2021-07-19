{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='organisation_id',
        cluster_by=['runstartedtime']
    )
}}
select organisation_id,organisation_name,attributes,max(loaded_timestamp) as loaded_timestamp,is_ghost,max(runstartedtime)  as runstartedtime
from (

    {{ ghost_organisation_entries(stg_of_fact_table='stg_act_mvt_orgdepotdaycase') }}

union
     {{ ghost_organisation_entries(stg_of_fact_table='stg_pln_mvt_daysku',org_col_name='SUBJECT_BUSINESS_ORGANISATION_NUMBER') }}

union
     {{ ghost_organisation_entries(stg_of_fact_table='stg_pln_mvt_orgdepotdaysku',org_col_name='SUBJECT_BUSINESS_ORGANISATION_NUMBER_TO') }}

union
     {{ ghost_organisation_entries(stg_of_fact_table='stg_pln_mvt_orgdepotdaysku',org_col_name='SUBJECT_BUSINESS_ORGANISATION_NUMBER_FROM') }}

) group by
    organisation_id,organisation_name,attributes,is_ghost