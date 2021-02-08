{{
    config(
        materialized='incremental',
        unique_key='location_ID',
        cluster_by=['loaded_timestamp']
    )
}}
with all_data as (
select
    location_ID,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_ID,
    ORGANISATION_LOCATION_ID,
    GEOGRAPHIC_LOCATION,
    LOCATION_FUNCTION,
    loaded_timestamp,
    false as is_ghost
from {{ ref('stg_location') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
),
ghost_data as (
select
    ghost_data.location_ID,
    ghost_data.organisation_ID,
    ghost_data.ORGANISATION_LOCATION_ID,
    TO_GEOGRAPHY(ghost_data.GEOGRAPHIC_LOCATION),
    ghost_data.LOCATION_FUNCTION,
    ghost_data.loaded_timestamp,
    ghost_data.is_ghost

from {{ ref('int_all_ghost_location') }} ghost_data
left join all_data
on all_data.location_ID = ghost_data.location_ID
  left join  {{ this }} stgloc
        on stgloc.location_ID = all_data.location_ID

where  stgloc.LOCATION_ID is null
and all_data.location_ID is null
)

select * from all_data
union
select * from ghost_data