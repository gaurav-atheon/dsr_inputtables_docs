{%- macro ghost_logisticitem_entries( stg_of_fact_table,prod_col_name='ORGANISATION_CASE') -%}

select  {{ dbt_utils.surrogate_key(['all_fact_data.origin_organisation_number','all_fact_data.business_organisation_number','all_fact_data.ORGANISATION_CASE']) }} as logisticitem_ID,
        all_fact_data.organisation_id,all_fact_data.product_ID,all_fact_data.ORGANISATION_SKU,all_fact_data.ORGANISATION_CASE,all_fact_data.CASE_SIZE,all_fact_data.GTIN,
        all_fact_data.loaded_timestamp,all_fact_data.is_ghost
from
(
select
    src.organisation_id,
    NULL as product_ID,
    NULL as ORGANISATION_SKU,
    inv.{{prod_col_name}} as ORGANISATION_CASE,
    NULL as CASE_SIZE,
    NULL as GTIN,
    inv.loaded_timestamp,
    business_organisation_number,
    origin_organisation_number,
    true as is_ghost

from {{ ref(stg_of_fact_table) }} inv
    inner join {{ ref('utl_source_organisations') }} src
on inv.source_db_id = src.business_organisation_number
    group by organisation_id,{{prod_col_name}},business_organisation_number,origin_organisation_number,loaded_timestamp
    ) all_fact_data

        {% if is_incremental() %}
        and all_fact_data.loaded_timestamp > (select max(loaded_timestamp) from {{ ref(stg_of_fact_table) }} )
        {% endif %}
{%- endmacro -%}


