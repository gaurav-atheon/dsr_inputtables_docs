{{
    config(
        materialized='incremental',
        unique_key='product_id',
        cluster_by=['loaded_timestamp']
    )
}}

select
    product_id,
    {{ dbt_utils.surrogate_key(['subject_origin_organisation_number','subject_business_organisation_number']) }} as creator_organisation_id,
    {{ dbt_utils.surrogate_key(['subject_origin_organisation_number','subject_business_organisation_number','grouping_key','loaded_timestamp']) }} as consumer_unit_id,
    loaded_timestamp
from {{ ref('stg_sku_grouping') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}