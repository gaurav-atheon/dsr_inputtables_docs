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
    subject_origin_organisation_number,
    subject_business_organisation_number,
    subject_organisation_location_id,
    subject_location_function,
    parent_origin_organisation_number,
    parent_business_organisation_number,
    parent_organisation_location_id,
    parent_location_function,
    creator_origin_organisation_number,
    creator_business_organisation_number,
    loaded_timestamp,
    created_timestamp,
    {{ dbt_utils.surrogate_key(['subject_origin_organisation_number','subject_business_organisation_number','subject_organisation_location_id',
                                'subject_location_function','creator_origin_organisation_number','creator_business_organisation_number']) }} as parentage_id,
    row_number() over (partition by subject_origin_organisation_number,subject_business_organisation_number,subject_organisation_location_id,
                                    subject_location_function,creator_origin_organisation_number,creator_business_organisation_number order by loaded_timestamp desc) rank
 {% if target.name == 'ci' %}
    from {{ ref ('stg_location_parentage_ci' )}}
 {% else %}
    from {{ source('dsr_input', 'input_location_parentage') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
 {% endif %}
)

select *
from ranked_data
where rank = 1