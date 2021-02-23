{{
    config(
        materialized='incremental',
        unique_key='organisation_id',
        cluster_by=['loaded_timestamp']
    )
}}
with all_data as (
select
    om.organisation_id,
    om.business_organisation_name as organisation_name,
    o.type,
    o.address,
    om.loaded_timestamp,
    false as is_ghost
from {{ ref('stg_organisation_mapping') }} om
left outer join {{ ref('stg_organisation') }} o
    on om.origin_organisation_number = o.origin_organisation_number
    and  om.business_organisation_number = o.business_organisation_number

        {% if is_incremental() %}
        where om.loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
),
ghost_data as (
select
       ghost_data.organisation_id,
       ghost_data.organisation_name,
       ghost_data.type,
       ghost_data.address,
       ghost_data.loaded_timestamp,
       ghost_data.is_ghost

from {{ ref('int_all_ghost_organisation') }} ghost_data

where

not exists
    (select 1
    from all_data
    where all_data.organisation_id = ghost_data.organisation_id)

    {% if is_incremental() %}
    and not exists
        (select 1
        from  {{ this }} dim
        where dim.organisation_id = ghost_data.organisation_id)
    {% endif %}

)

select * from all_data
union
select * from ghost_data

