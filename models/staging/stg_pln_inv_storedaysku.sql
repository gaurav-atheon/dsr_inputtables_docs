{{
    config(
        materialized='incremental',
        unique_key='unique_pln_inv_storedaysku',
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
    ranged,
    loaded_timestamp,
{{ dbt_utils.surrogate_key(['day_date','source_db_id','organisation_location_id','Organisation_SKU','ranged']) }} as unique_pln_inv_storedaysku,
    row_number() over (partition by day_date, source_db_id, organisation_location_id, Organisation_SKU,ranged order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_pln_inv_storedaysku') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1