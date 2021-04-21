{{
    config(
        materialized='incremental',
        unique_key='location_id',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    organisation_location_id,
    origin_organisation_number,
    business_organisation_number,
    geographic_location,
    location_function,
    attributes,
    loaded_timestamp,
    created_timestamp,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','organisation_location_id','location_function']) }} as location_id,
    row_number() over (partition by origin_organisation_number,business_organisation_number,organisation_location_id order by loaded_timestamp desc) rank
 {% if target.name == 'ci' %}
    from {{ ref ('stg_location_ci' )}}
 {% else %}
    from {{ source('dsr_input', 'input_location') }}
        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}
 {% endif %}
)

select *
from ranked_data
where rank = 1