{{
    config(
        materialized='incremental',
        unique_key='organisation_id',
        cluster_by=['loaded_timestamp']
    )
}}
 {% if target.name == 'ci' %}
        with all_data as
        (
        {{ dbt_utils.union_relations(
            relations=[ref('master_organisation'), ref ('stg_organisation_mapping_ci' )],
        ) }}
        ),
 {% else %}
        with all_data as
        (
        {{ dbt_utils.union_relations(
            relations=[ref('master_organisation'), source('dsr_input', 'input_organisation_mapping')],
        ) }}
        ),
 {% endif %}


ranked_data as
(
select
    a.business_organisation_number,
    a.business_organisation_name,
    a.origin_organisation_number,
    a.loaded_timestamp,
    a.created_timestamp,
    '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/London")) }}'  as runstartedtime,
  {{ dbt_utils.surrogate_key(['a.origin_organisation_number','a.business_organisation_number']) }} as organisation_id,
    row_number() over (partition by a.origin_organisation_number, a.business_organisation_number order by a.loaded_timestamp desc) rank
from all_data a
)

select *
from ranked_data
where rank = 1