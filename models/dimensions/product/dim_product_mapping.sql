{{
    config(
        materialized='incremental',
        unique_key='Product_ID',
        cluster_by=['loaded_timestamp']
    )
}}

select
    Product_ID,
    {{ dbt_utils.surrogate_key(['DECISION_MAKER_ORIGIN_ORGANISATION_NUMBER','DECISION_MAKER_ORGANISATION_NUMBER']) }} as Decision_maker_organisation_ID,
    {{ dbt_utils.surrogate_key(['DECISION_MAKER_ORIGIN_ORGANISATION_NUMBER','DECISION_MAKER_ORGANISATION_NUMBER','GROUPING_KEY','loaded_timestamp']) }} as consumer_unit_id,
    loaded_timestamp
from {{ ref('stg_sku_grouping') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}