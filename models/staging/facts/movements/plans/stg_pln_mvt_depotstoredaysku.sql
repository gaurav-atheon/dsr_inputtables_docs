{{
    config(
        materialized='incremental',
        unique_key='unique_pln_mvt_depotstoredaysku',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    base_forecast_date as  day_date,
    forecast_date,
    source_db_id,
    organisation_location_id_from,
    organisation_location_id_to,
    organisation_sku,
    units_required,
    units_required_upper,
    units_required_lower,
    forecast_components,
    created_at,
    model_version,
    origin_file,
    loaded_timestamp,
    created_timestamp,
   {{ dbt_utils.surrogate_key(['base_forecast_date','forecast_date','source_db_id','organisation_location_id_from',
                                'organisation_location_id_to','organisation_sku','model_version']) }} as unique_pln_mvt_depotstoredaysku,
    row_number() over (partition by base_forecast_date, forecast_date, source_db_id, organisation_location_id_from,
                                    organisation_location_id_to,organisation_sku,model_version order by loaded_timestamp desc) rank

from {{ source('dsr_input', 'input_pln_mvt_depotstoredaysku') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1