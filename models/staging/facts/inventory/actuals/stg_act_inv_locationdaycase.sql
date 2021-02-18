{{
    config(
        materialized='incremental',
        unique_key='unique_act_inv_locationdaycase',
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
    organisation_case,
    stock_units,
    location_function,
    loaded_timestamp,
       'case' as source,
    {{ dbt_utils.surrogate_key(['day_date','source_db_id','organisation_location_id','organisation_case','location_function']) }} as unique_act_inv_locationdaycase,
    row_number() over (partition by day_date, source_db_id, organisation_location_id, organisation_case,location_function order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_act_inv_locationdaycase') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1