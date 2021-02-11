{{
    config(
        materialized='incremental',
        unique_key='product_id',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    organisation_sku,
    origin_organisation_number,
    business_organisation_number,
    decision_maker_origin_organisation_number,
    decision_maker_organisation_number,
    grouping_key,
    loaded_timestamp,
       {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','organisation_sku']) }} as product_id,
    row_number() over (partition by origin_organisation_number,business_organisation_number,organisation_sku,
                                    decision_maker_origin_organisation_number,decision_maker_organisation_number order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_sku_grouping') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1