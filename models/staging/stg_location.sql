{{
    config(
        materialized='incremental',
        unique_key='location_ID',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    ORGANISATION_LOCATION_ID,
    origin_organisation_number,
    business_organisation_number,
    GEOGRAPHIC_LOCATION,
    LOCATION_FUNCTION,
    loaded_timestamp,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','ORGANISATION_LOCATION_ID','LOCATION_FUNCTION']) }} as location_ID,
    row_number() over (partition by origin_organisation_number,business_organisation_number,ORGANISATION_LOCATION_ID order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_location') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1