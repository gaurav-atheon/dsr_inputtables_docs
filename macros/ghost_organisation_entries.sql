{%- macro ghost_organisation_entries( stg_of_fact_table,org_col_name='BUSINESS_ORGANISATION_NUMBER_FROM') -%}

select     {{ dbt_utils.surrogate_key(['origin_organisation_number','business_organisation_number']) }} as organisation_id,all_fact_data.organisation_name,
            all_fact_data.attributes,all_fact_data.loaded_timestamp,all_fact_data.is_ghost,all_fact_data.runstartedtime
from
(
select
  null as organisation_name,
  null as attributes,
  inv.{{org_col_name}} as business_organisation_number,
  to_timestamp(inv.loaded_timestamp) as loaded_timestamp,
  inv.source_db_id as origin_organisation_number,
  true as is_ghost,
  runstartedtime

from {{ ref(stg_of_fact_table) }} inv
    inner join {{ ref('utl_source_organisations') }} src
on inv.source_db_id = src.business_organisation_number
    group by organisation_name,{{org_col_name}},loaded_timestamp,source_db_id,is_ghost,runstartedtime
    ) all_fact_data

    {% if is_incremental() %}
      where all_fact_data.runstartedtime > nvl((select max(runstartedtime) from {{ this }}), to_timestamp('0'))
    {% endif %}
{%- endmacro -%}