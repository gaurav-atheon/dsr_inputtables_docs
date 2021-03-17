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
    creator_origin_organisation_number,
    creator_business_organisation_number,
    subject_origin_organisation_number,
    subject_business_organisation_number,
    grouping_key,
    loaded_timestamp,
    created_timestamp,
       {{ dbt_utils.surrogate_key(['creator_origin_organisation_number','creator_business_organisation_number','organisation_sku']) }} as product_id,
    row_number() over (partition by creator_origin_organisation_number,creator_business_organisation_number,organisation_sku,
                                    subject_origin_organisation_number,subject_business_organisation_number order by loaded_timestamp desc) rank
 {% if target.name == 'ci' %}
    from {{ ref ('stg_sku_grouping_ci' )}}
 {% else %}
    from {{ source('dsr_input', 'input_sku_grouping') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
 {% endif %}
)

select *
from ranked_data
where rank = 1