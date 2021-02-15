{% macro fact_visibility(staging_table, fact_table, sku_or_case, access_level, org_column, location_function=NULL) %}

    select
ord.day_date,

    {% if sku_or_case =='sku' %}
        prd.Product_ID as item_id, --converted to DSR ID
    {% elif sku_or_case == 'case' %}
        prd.logisticitem_ID as item_id, --converted to DSR ID,
    {% endif %}

    {% if  location_function %}
        loc.LOCATION_FUNCTION as location_function,
    {% else %}
        NULL as location_function,
    {% endif %}

src.organisation_id, --converted to DSR ID
'{{fact_table}}' table_reference,
'{{access_level}}' access_level,
max(ord.loaded_timestamp) as loaded_timestamp

from {{ ref(staging_table) }} ord

    {% if org_column == 'source_db_id' %}
        inner join {{ ref('utl_source_organisations') }} src
        on ord.source_db_id = src.business_organisation_number
    {% else %}
        inner join {{ ref('dim_organisation_mapping') }} src
        on ord.{{- org_column }} = src.business_organisation_number
        and ord.source_db_id = src.origin_organisation_number
    {% endif %}

    {% if  location_function %}
        inner join {{ ref('dim_location') }} loc
        on ord.location_ID = loc.location_ID
    {% endif %}

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

    {% if  sku_or_case == 'sku' %}
        prd.Product_ID,
    {% elif  sku_or_case == 'case' %}
        prd.logisticitem_ID,
    {% endif %}

    {% if  location_function %}
        loc.LOCATION_FUNCTION,
    {% else %}
        NULL,
    {% endif %}

src.organisation_id

{% endmacro %}