{{
    config(
        materialized='incremental',
        unique_key='unique_key',
        cluster_by=['loaded_timestamp', 'table_reference']
    )
}}

{% set base_facts = [
                        {'fact':'act_mvt_orgdepotdaysku', 'product_type':'sku', 'org_column':'BUSINESS_ORGANISATION_NUMBER_FROM', 'location_flag':true, 'access_level':'200 - Implicit'}
                        {'fact':'act_mvt_storedaysku', 'product_type':'sku', 'org_column':'source_db_id', 'location_flag':NULL, 'access_level':'100 - Explicit'},
                        {'fact':'act_mvt_depotstoredaysku', 'product_type':'sku', 'org_column':'source_db_id', 'location_flag':NULL, 'access_level':'100 - Explicit'},
                        {'fact':'act_inv_locationdaysku', 'product_type':'sku', 'org_column':'source_db_id', 'location_flag':true, 'access_level':'100 - Explicit'},
                        {'fact':'pln_inv_storedaysku', 'product_type':'sku', 'org_column':'source_db_id', 'location_flag':NULL, 'access_level':'100 - Explicit'},
                        {'fact':'pln_mvt_depotstoredaysku', 'product_type':'sku', 'org_column':'source_db_id', 'location_flag':NULL, 'access_level':'100 - Explicit'},
                        {'fact':'act_mvt_orgdepotdaycase', 'product_type':'case', 'org_column':'source_db_id', 'location_flag':NULL, 'access_level':'100 - Explicit'},
                        {'fact':'act_inv_locationdaycase', 'product_type':'case', 'org_column':'source_db_id', 'location_flag':true, 'access_level':'100 - Explicit'},
                        {'fact':'act_mvt_orgdepotdaycase', 'product_type':'case', 'org_column':'BUSINESS_ORGANISATION_NUMBER_FROM', 'location_flag':true, 'access_level':'200 - Implicit'}
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

{% for base_fact in base_facts %}
    {% set fact = "fact_" + base_fact['fact'] %}
    {% set staging = "stg_" + base_fact['fact'] %}
    select day_date, organisation_id, item_id, location_function, table_reference, access_level, loaded_timestamp
    from (
    {{ fact_visibility(
        staging_table = staging,
        fact_table=fact,
        sku_or_case=base_fact['product_type'],
        location_function=base_fact['location_flag'],
        access_level=base_fact['access_level'],
        org_column=base_fact['org_column'])  }}
    )

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }} where table_reference = '{{ fact }}' )
        {% endif %}

    {%- if not loop.last %}
        union all
    {% endif %}
{% endfor %}

)