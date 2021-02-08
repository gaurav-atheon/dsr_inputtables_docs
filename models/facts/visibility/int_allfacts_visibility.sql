{{
    config(
        materialized='incremental',
        unique_key='unique_key'
    )
}}

{% set explicit_sku_facts = ['act_mvt_storedaysku', 'act_mvt_depotstoredaysku', 'act_inv_locationdaysku', 'pln_inv_storedaysku', 'pln_mvt_depotstoredaysku'] %}
{% set explicit_case_facts = ['act_mvt_orgdepotdaycase', 'act_inv_locationdaycase'] %}

select
day_date,
organisation_id,
item_id,
table_reference,
access_level,
loaded_timestamp,
{{ dbt_utils.surrogate_key(['day_date','organisation_id','item_id','table_reference','access_level']) }} as unique_key
from
(

{% for explicit_sku_fact in explicit_sku_facts %}
    {% set fact = "fact_" + explicit_sku_fact %}
    {% set staging = "stg_" + explicit_sku_fact %}
    select day_date, organisation_id, item_id, table_reference, access_level, loaded_timestamp
    from (
    {{ fact_visibility(staging_table = staging, fact_table=fact, sku_or_case="sku", access_level="100 - Explicit")  }}
    )
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }} where table_reference = '{{ fact }}' )
        {% endif %}
    {%- if not loop.last %}
        union all
    {% endif %}
{% endfor %}

{% if explicit_sku_facts is defined %}
    {% if explicit_case_facts is defined %}
    union all
    {% endif %}
{% endif %}

{% for explicit_case_fact in explicit_case_facts %}
    {% set fact = "fact_" + explicit_case_fact %}
    {% set staging = "stg_" + explicit_case_fact %}
    select day_date, organisation_id, item_id, table_reference, access_level, loaded_timestamp
    from (
    {{ fact_visibility(staging_table = staging, fact_table=fact, sku_or_case="case", access_level="100 - Explicit")  }}
    )
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }} where table_reference = '{{ fact }}' )
        {% endif %}
    {%- if not loop.last %}
        union all
    {% endif %}
{% endfor %}

)