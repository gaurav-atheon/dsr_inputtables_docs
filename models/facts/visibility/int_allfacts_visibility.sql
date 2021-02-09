{{
    config(
        materialized='incremental',
        unique_key='unique_key'
    )
}}

{% set explicit_facts = [
                        {'fact':'act_mvt_storedaysku', 'product_type':'sku', 'location_flag':NULL},
                        {'fact':'act_mvt_depotstoredaysku', 'product_type':'sku', 'location_flag':NULL},
                        {'fact':'act_inv_locationdaysku', 'product_type':'sku', 'location_flag':TRUE},
                        {'fact':'pln_inv_storedaysku', 'product_type':'sku', 'location_flag':NULL},
                        {'fact':'pln_mvt_depotstoredaysku', 'product_type':'sku', 'location_flag':NULL},
                        {'fact':'act_mvt_orgdepotdaycase', 'product_type':'case', 'location_flag':NULL},
                        {'fact':'act_inv_locationdaycase', 'product_type':'case', 'location_flag':TRUE}
                        ] %}

select
day_date,
organisation_id,
item_id,
location_function,
table_reference,
access_level,
loaded_timestamp,
{{ dbt_utils.surrogate_key(['day_date','organisation_id','item_id','table_reference','location_function','access_level']) }} as unique_key
from
(

{% for explicit_fact in explicit_facts %}
    {% set fact = "fact_" + explicit_fact['fact'] %}
    {% set staging = "stg_" + explicit_fact['fact'] %}
    select day_date, organisation_id, item_id, location_function, table_reference, access_level, loaded_timestamp
    from (
    {{ fact_visibility(staging_table = staging, fact_table=fact, sku_or_case=explicit_fact['product_type'], location_function=explicit_fact['location_flag'], access_level="100 - Explicit")  }}
    )
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }} where table_reference = '{{ fact }}' )
        {% endif %}
    {%- if not loop.last %}
        union all
    {% endif %}
{% endfor %}

)