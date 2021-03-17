{{
    config(
        materialized='incremental',
        unique_key='logisticitem_id',
        cluster_by=['loaded_timestamp']
    )
}}
with ranked_data as
(select
    organisation_sku,
    origin_organisation_number,
    business_organisation_number,
    organisation_case,
    case_size,
    gtin,
    loaded_timestamp,
    created_timestamp,
    row_number() over (partition by origin_organisation_number,business_organisation_number,organisation_case order by loaded_timestamp desc) rank,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','organisation_case']) }} as logisticitem_id
 {% if target.name == 'ci' %}
    from {{ ref ('stg_case_ci' )}}
 {% else %}
    from {{ source('dsr_input', 'input_case') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
 {% endif %}
)

select *
from ranked_data
where rank = 1

