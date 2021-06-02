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
    description,
    origin_organisation_number,
    business_organisation_number,
    individual_units,
    net_quantity,
    base_unit,
    brand,
    gtin,
    loaded_timestamp,
    attributes,
    created_timestamp,
    '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/London")) }}'  as runstartedtime,
    {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number','organisation_sku']) }} as product_id,
    row_number() over (partition by origin_organisation_number,business_organisation_number,organisation_sku order by loaded_timestamp desc) rank
 {% if target.name == 'ci' %}
    from {{ ref ('stg_sku_ci' )}}
 {% else %}
    from {{ source('dsr_input', 'input_sku') }}
        {% if is_incremental() %}
        where loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}
 {% endif %}
)

select *
from ranked_data
where rank = 1