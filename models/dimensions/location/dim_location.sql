{{
    config(
        materialized='incremental',
        unique_key='location_ID',
        cluster_by=['loaded_timestamp']
    )
}}
with all_data as (
select
    location_id,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_id,
    organisation_location_id,
    geographic_location,
    location_function,
    attributes,
    loaded_timestamp,
    false as is_ghost
from {{ ref('stg_location') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
),
ghost_data as (
select
    ghost_data.location_id,
    ghost_data.organisation_id,
    ghost_data.organisation_location_id,
    to_geography(ghost_data.geographic_location),
    ghost_data.location_function,
    to_variant(ghost_data.attributes),
    ghost_data.loaded_timestamp,
    ghost_data.is_ghost

from {{ ref('int_all_ghost_location') }} ghost_data

where

NOT EXISTS
    (select 1
    from all_data
    where all_data.location_ID = ghost_data.location_ID)

    {% if is_incremental() %}
    and NOT EXISTS
        (select 1
        from  {{ this }} dim
        where dim.location_ID = ghost_data.location_ID)
    {% endif %}

)

select * from all_data
union
select * from ghost_data