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
    organisation_location_id,
    organisation_sku,
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
    created_timestamp,
    '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/London")) }}'  as runstartedtime,
    {{ dbt_utils.surrogate_key(['day_date','source_db_id','organisation_location_id','organisation_sku']) }} as unique_key,
    row_number() over (partition by day_date, source_db_id, organisation_location_id, organisation_sku order by loaded_timestamp desc) rank
 {% if target.name == 'ci' %}
    from {{ ref ('stg_act_mvt_storedaysku_ci' )}}
 {% else %}
     from {{ source('dsr_input', 'input_act_mvt_storedaysku') }}
            {% if is_incremental() %}
            where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
            {% endif %}
 {% endif %}

)

select *
from ranked_data
where rank = 1