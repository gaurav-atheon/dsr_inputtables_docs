{%- macro ghost_product_entries( stg_of_fact_table,prod_col_name='ORGANISATION_SKU') -%}

select  {{ dbt_utils.surrogate_key(['all_fact_data.origin_organisation_number','all_fact_data.business_organisation_number','all_fact_data.organisation_sku']) }} as product_id,
        all_fact_data.organisation_id,all_fact_data.organisation_sku,all_fact_data.description,all_fact_data.individual_units,all_fact_data.net_quantity,
        all_fact_data.base_unit,all_fact_data.brand,all_fact_data.gtin, all_fact_data.loaded_timestamp,all_fact_data.attributes,all_fact_data.is_ghost,all_fact_data.runstartedtime
from
(
select
    src.organisation_id,
    inv.{{prod_col_name}} as organisation_sku,
    null as description,
    null as individual_units,
    null as net_quantity,
    null as base_unit,
    null as brand,
    null as gtin,
    to_timestamp(inv.loaded_timestamp) as loaded_timestamp,
    business_organisation_number,
    origin_organisation_number,
    null as attributes,
    true as is_ghost,
    runstartedtime

from {{ ref(stg_of_fact_table) }} inv
    inner join {{ ref('utl_source_organisations') }} src
on inv.source_db_id = src.business_organisation_number
    group by organisation_id,{{prod_col_name}},business_organisation_number,origin_organisation_number,loaded_timestamp,runstartedtime
    ) all_fact_data

    {% if is_incremental() %}
        where all_fact_data.runstartedtime > nvl((select max(runstartedtime) from {{ this }}), to_timestamp('0'))
    {% endif %}
{%- endmacro -%}


