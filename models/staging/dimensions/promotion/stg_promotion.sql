{{
    config(
        materialized='table'
    )
}}
with
ranked_data as
(
select
   creator_business_organisation_number,
   creator_origin_organisation_number,
   subject_origin_organisation_number,
   subject_business_organisation_number,
   promotion_number,
   start_date,
   end_date,
   organisation_sku,
   attributes,
   created_timestamp,
   loaded_timestamp,
    '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/London")) }}'  as runstartedtime,
   {{ dbt_utils.surrogate_key(['creator_business_organisation_number','creator_origin_organisation_number','subject_business_organisation_number',
                                'subject_origin_organisation_number','promotion_number','organisation_sku']) }} as promotion_id,
    row_number() over (partition by creator_business_organisation_number,creator_origin_organisation_number,subject_origin_organisation_number,
                                    subject_business_organisation_number,promotion_number,organisation_sku order by loaded_timestamp desc) rank
 {% if target.name == 'ci' %}
    from {{ ref ('stg_promotion_ci' )}}
 {% else %}
    from {{ source('dsr_input', 'input_promotion') }}
        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}
 {% endif %}
)

select *
from ranked_data
where rank = 1