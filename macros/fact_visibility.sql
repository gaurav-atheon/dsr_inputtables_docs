{% macro fact_visibility(staging_table, fact_table, sku_or_case, access_level) %}

    select
ord.day_date,
src.organisation_id, --converted to DSR ID

    {% if sku_or_case =='sku' %}
        prd.Product_ID as item_id, --converted to DSR ID
    {% elif sku_or_case == 'case' %}
        prd.logisticitem_ID as item_id, --converted to DSR ID,
    {% endif %}

'{{fact_table}}' table_reference,
'{{access_level}}' access_level,
ord.loaded_timestamp

from {{ ref(staging_table) }} ord

inner join {{ ref('utl_source_organisations') }} src
on ord.source_db_id = src.business_organisation_number

    {% if  sku_or_case == 'sku' %}
        inner join {{ ref('dim_product') }} prd
        on prd.organisation_ID = src.organisation_ID
        and prd.ORGANISATION_SKU = ord.ORGANISATION_SKU
    {% elif  sku_or_case == 'case' %}
        inner join {{ ref('dim_logisticitem') }} prd
        on prd.organisation_ID = src.organisation_ID
        and prd.ORGANISATION_case = ord.ORGANISATION_case
    {% endif %}

group by
ord.day_date,
src.organisation_id,

    {% if  sku_or_case == 'sku' %}
        prd.Product_ID,
    {% elif  sku_or_case == 'case' %}
        prd.logisticitem_ID,
    {% endif %}

ord.loaded_timestamp

{% endmacro %}