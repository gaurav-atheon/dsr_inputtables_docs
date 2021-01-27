{{
    config(
        materialized='incremental',
        unique_key='logisticitem_ID',
        cluster_by=['loaded_timestamp']
    )
}}
with ranked_data as
(select
    ORGANISATION_SKU,
    origin_organisation_number,
    business_organisation_number,
    ORGANISATION_CASE,
    CASE_SIZE,
    GTIN,
    loaded_timestamp,
    row_number() over (partition by origin_organisation_number,business_organisation_number,ORGANISATION_CASE order by loaded_timestamp desc) rank,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','ORGANISATION_CASE']) }} as logisticitem_ID
from {{ source('dsr_input', 'input_case') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1

