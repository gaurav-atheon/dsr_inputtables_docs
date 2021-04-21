{{
    config(
        materialized='incremental',
        unique_key='organisation_id',
        cluster_by=['loaded_timestamp']
    )
}}
with
ranked_data as
(
select
    origin_organisation_number,
    business_organisation_number,
    attributes,
    loaded_timestamp,
    created_timestamp,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_id,
    row_number() over (partition by origin_organisation_number,business_organisation_number order by loaded_timestamp desc) rank
 {% if target.name == 'ci' %}
    from {{ ref ('stg_organisation_ci' )}}
 {% else %}
    from {{ source('dsr_input', 'input_organisation') }}
        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}
 {% endif %}
)

select *
from ranked_data
where rank = 1