{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='parentage_id',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    op.origin_organisation_number,
    op.business_organisation_number,
    op.parent_origin_organisation_number,
    op.parent_business_organisation_number,
    op.creator_origin_organisation_number,
    op.creator_business_organisation_number,
    op.loaded_timestamp,
    op.created_timestamp,
    '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/London")) }}'  as runstartedtime,
    {{ dbt_utils.surrogate_key(['op.origin_organisation_number','op.business_organisation_number', 
                                'op.parent_origin_organisation_number','op.parent_business_organisation_number',
                                'op.creator_origin_organisation_number','op.creator_business_organisation_number']) }} as parentage_id,
    row_number() over (partition by op.origin_organisation_number,op.business_organisation_number,
                                    op.parent_origin_organisation_number,op.parent_business_organisation_number,
                                    op.creator_origin_organisation_number,op.creator_business_organisation_number order by op.loaded_timestamp desc) rank
 {% if target.name == 'ci' %}
    from {{ ref ('stg_organisation_parentage_ci' )}} op
 {% else %}
    from {{ source('dsr_input', 'input_organisation_parentage') }} op
    inner join {{ ref ('stg_organisation_mapping' )}} om
    on op.parent_business_organisation_number = om.business_organisation_number
    and op.parent_origin_organisation_number = om.origin_organisation_number
        {% if is_incremental() %}
        where op.loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}
 {% endif %}
)

select *
from ranked_data
where rank = 1