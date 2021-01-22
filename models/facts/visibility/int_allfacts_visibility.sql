{{
    config(
        materialized='incremental',
        unique_key='unique_key'
    )
}}


select *
from {{ ref('int_storedaysku_orders_visibility') }}

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }} where table_reference = 'fact_storedaysku_orders' )
        {% endif %}}