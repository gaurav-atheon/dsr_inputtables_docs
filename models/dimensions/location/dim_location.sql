{{
    config(
        materialized='incremental',
        unique_key='location_ID',
        cluster_by=['loaded_timestamp']
    )
}}

select
    location_ID,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_ID,
    ORGANISATION_LOCATION_ID,
    GEOGRAPHIC_LOCATION,
    LOCATION_FUNCTION,
    loaded_timestamp
from {{ ref('stg_location') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}