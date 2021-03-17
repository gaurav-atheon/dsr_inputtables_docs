{{
    config(
        materialized='incremental',
        unique_key='unique_key',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    day_date,
    source_db_id,
    organisation_location_id_from,
    organisation_location_id_to,
    organisation_sku,
    units_ordered,
    units_fulfilled,
    loaded_timestamp,
    created_timestamp,
    {{ dbt_utils.surrogate_key(['day_date','source_db_id','organisation_location_id_from','organisation_location_id_to','organisation_sku']) }} as unique_key,
    row_number() over (partition by day_date, source_db_id, organisation_location_id_from,organisation_location_id_to, organisation_sku order by loaded_timestamp desc) rank

 {% if target.name == 'ci' %}
    from {{ ref ('stg_act_mvt_depotstoredaysku_ci' )}}
 {% else %}
     from {{ source('dsr_input', 'input_act_mvt_depotstoredaysku') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
 {% endif %}
)

select *
from ranked_data
where rank = 1