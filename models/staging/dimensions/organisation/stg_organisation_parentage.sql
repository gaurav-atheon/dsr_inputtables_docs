{{
    config(
        materialized='incremental',
        unique_key='parentage_id',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    origin_organisation_number,
    business_organisation_number,
    parent_origin_organisation_number,
    parent_business_organisation_number,
    creator_origin_organisation_number,
    creator_business_organisation_number,
    loaded_timestamp,
    created_timestamp,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number',
                                'creator_origin_organisation_number','creator_business_organisation_number']) }} as parentage_id,
    row_number() over (partition by origin_organisation_number,business_organisation_number,
                                    creator_origin_organisation_number,creator_business_organisation_number order by loaded_timestamp desc) rank
from {{ source('dsr_input', 'input_organisation_parentage') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
)

select *
from ranked_data
where rank = 1