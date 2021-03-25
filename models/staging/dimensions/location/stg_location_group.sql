{{
    config(
        materialized='incremental',
        unique_key='location_mapping_id',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    organisation_location_id,
    creator_origin_organisation_number,
    creator_business_organisation_number,
    subject_origin_organisation_number,
    subject_business_organisation_number,
    location_function,
    group_name,
    group_value,
    loaded_timestamp,
    created_timestamp,
    {{ dbt_utils.surrogate_key(['creator_origin_organisation_number','creator_business_organisation_number','group_name','organisation_location_id',
                                'subject_origin_organisation_number','subject_business_organisation_number','location_function']) }} as location_mapping_id,
    row_number() over (partition by creator_origin_organisation_number,creator_business_organisation_number,group_name,organisation_location_id,
                                subject_origin_organisation_number,subject_business_organisation_number,location_function order by loaded_timestamp desc) rank
 {% if target.name == 'ci' %}
    from {{ ref ('stg_location_group_ci' )}}
 {% else %}
    from {{ source('dsr_input', 'input_location_group') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}
 {% endif %}

)

select *
from ranked_data
where rank = 1