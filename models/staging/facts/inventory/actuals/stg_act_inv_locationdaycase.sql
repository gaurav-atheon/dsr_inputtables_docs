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
    organisation_case,
    stock_units,
    location_function,
    loaded_timestamp,
       'case' as source,
    created_timestamp,
    {{ dbt_utils.surrogate_key(['day_date','source_db_id','organisation_location_id','organisation_case','location_function']) }} as unique_key,
    row_number() over (partition by day_date, source_db_id, organisation_location_id, organisation_case,location_function order by loaded_timestamp desc) rank
 {% if target.name == 'ci' %}
    from {{ ref ('stg_act_inv_locationdaycase_ci' )}}
 {% else %}
    from {{ source('dsr_input', 'input_act_inv_locationdaycase') }}
        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}
 {% endif %}
)

select *
from ranked_data
where rank = 1