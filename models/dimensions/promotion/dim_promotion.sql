{{
    config(
        materialized='incremental',
        unique_key='promotion_id',
        cluster_by=['loaded_timestamp']
    )
}}
with all_data as (
select
    promotion_id,
    {{ dbt_utils.surrogate_key(['creator_origin_organisation_number','creator_business_organisation_number']) }} as creator_organisation_id,
    {{ dbt_utils.surrogate_key(['subject_origin_organisation_number','subject_business_organisation_number']) }} as subject_organisation_id,
    promotion_number,
    start_date,
    end_date,
    {{ dbt_utils.surrogate_key(['subject_origin_organisation_number','subject_business_organisation_number','organisation_sku']) }} as product_id,
    attributes,
    created_timestamp,
    loaded_timestamp,
    false as is_ghost
from {{ ref('stg_promotion') }}

        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}
)

select * from all_data


