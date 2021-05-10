{{
    config(
        materialized='incremental',
        unique_key='unique_key',
        cluster_by=['loaded_timestamp', 'table_reference']
    )
}}

{% set base_facts = [
                        {'fact':'act_mvt_storedaysku', 'product_type':'sku', 'org_column':'organisation_id', 'org_location_column':'organisation_id', 'location_flag':NULL, 'access_level':'100 - Explicit'},
                        {'fact':'act_mvt_depotstoredaysku', 'product_type':'sku', 'org_column':'organisation_id', 'org_location_column':'organisation_id', 'location_flag':NULL, 'access_level':'100 - Explicit'},
                        {'fact':'act_inv_locationdaysku', 'product_type':'sku', 'org_column':'organisation_id', 'org_location_column':'organisation_id', 'location_flag':true, 'access_level':'100 - Explicit'},
                        {'fact':'pln_inv_storedaysku', 'product_type':'sku', 'org_column':'organisation_id', 'org_location_column':'organisation_id', 'location_flag':NULL, 'access_level':'100 - Explicit'},
                        {'fact':'pln_mvt_depotstoredaysku', 'product_type':'sku', 'org_column':'organisation_id', 'org_location_column':'organisation_id', 'location_flag':NULL, 'access_level':'100 - Explicit'},
                        {'fact':'act_mvt_orgdepotdaycase', 'product_type':'case', 'org_column':'organisation_id_to', 'org_location_column':'organisation_id_to', 'location_flag':NULL, 'access_level':'100 - Explicit'},
                        {'fact':'act_inv_locationdaycase', 'product_type':'case', 'org_column':'organisation_id', 'org_location_column':'organisation_id', 'location_flag':true, 'access_level':'100 - Explicit'},
                        {'fact':'act_mvt_orgdepotdaycase', 'product_type':'case', 'org_column':'organisation_id_from', 'org_location_column':'organisation_id_to', 'location_flag':NULL, 'access_level':'200 - Implicit'},
                        {'fact':'act_mvt_orgdepotdaysku', 'product_type':'sku', 'org_column':'organisation_id_from', 'org_location_column':'organisation_id_to', 'location_flag':NULL, 'access_level':'200 - Implicit'},
                        {'fact':'pln_mvt_orgdepotdaysku', 'product_type':'sku', 'org_column':'subject_organisation_id_to', 'org_location_column':'organisation_id_to', 'location_flag':NULL, 'access_level':'100 - Explicit'},
                        {'fact':'pln_mvt_orgdepotdaysku', 'product_type':'sku', 'org_column':'subject_organisation_id_from', 'org_location_column':'organisation_id_to', 'location_flag':NULL, 'access_level':'200 - Implicit'}
                        ] %}

select
day_date,
organisation_id,
product_type,
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
    {% set access_level = base_fact['access_level'] %}
    {% set sku_or_case = base_fact['product_type'] %}
    {% set location_function=base_fact['location_flag'] %}
    {% set product_type=base_fact['product_type'] %}

    select day_date, organisation_id, product_type, item_id, location_function, table_reference, access_level, loaded_timestamp
    from (

        select
        ord.day_date,
        '{{product_type}}' as product_type,

    {% if sku_or_case =='sku' %}
        ord.Product_ID as item_id, --converted to DSR ID
    {% elif sku_or_case == 'case' %}
        ord.logisticitem_ID as item_id, --converted to DSR ID,
    {% endif %}

    {% if location_function %}
        loc.LOCATION_FUNCTION as location_function,
    {% else %}
        NULL as location_function,
    {% endif %}

    {{ "ord." +base_fact['org_column'] }} as organisation_id,
    '{{fact}}' as table_reference,
    '{{access_level}}' as access_level,
    max(ord.loaded_timestamp) as loaded_timestamp

    from {{ ref(fact) }} ord

    {% if location_function %}
        inner join {{ ref('dim_location') }} loc
        on loc.organisation_ID = {{ "ord." +base_fact['org_location_column'] }}
        and loc.LOCATION_ID = ord.LOCATION_ID
    {% endif %}

    group by
    ord.day_date,
    '{{product_type}}',

    {% if sku_or_case =='sku' %}
        ord.Product_ID, --converted to DSR ID
    {% elif sku_or_case == 'case' %}
        ord.logisticitem_ID, --converted to DSR ID,
    {% endif %}

    {% if location_function %}
        loc.LOCATION_FUNCTION,
    {% else %}
        NULL,
    {% endif %}

    {{ "ord." +base_fact['org_column'] }},
    '{{fact}}',
    '{{access_level}}'
    )

    where item_id is not null -- some case conversions cannot happen until new dimension data comes through

        {% if is_incremental() %}
        and loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }} where table_reference = '{{ fact }}' ), to_timestamp('0'))
        {% endif %}

    {%- if not loop.last %}
        union all
    {% endif %}
{% endfor %}

)