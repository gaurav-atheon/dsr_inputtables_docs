{{
    config(
        materialized='incremental',
        unique_key='unique_act_mvt_storedaysku',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    day_date,
    source_db_id,
    organisation_location_id,
    Organisation_SKU,
    epos_eaches,
    epos_value,
    rtc_epos_eaches,
    rtc_epos_value,
    rtc_reduction_value,
    bin_waste_eaches,
    bin_waste_value,
    total_waste_eaches,
    total_waste_value,
    loaded_timestamp,
    {{ dbt_utils.surrogate_key(['day_date','source_db_id','organisation_location_id','Organisation_SKU']) }} as unique_act_mvt_storedaysku,
    row_number() over (partition by day_date, source_db_id, organisation_location_id, Organisation_SKU order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_act_mvt_storedaysku') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1