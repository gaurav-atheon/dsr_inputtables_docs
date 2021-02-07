{%- macro ghost_location_entries( stg_of_fact_table,location_col_name='ORGANISATION_LOCATION_ID',fact_location_function='') -%}

select     {{ dbt_utils.surrogate_key(['all_fact_data.origin_organisation_number', 'all_fact_data.business_organisation_number',
                                        'all_fact_data.organisation_location_id', 'all_fact_data.location_function']) }} as location_ID,
            all_fact_data.organisation_id,all_fact_data.ORGANISATION_LOCATION_ID,all_fact_data.GEOGRAPHIC_LOCATION,
            all_fact_data.location_function,all_fact_data.loaded_timestamp,all_fact_data.is_ghost
from
(
select
    src.organisation_id,
    inv.{{location_col_name}} as ORGANISATION_LOCATION_ID,
    NULL as GEOGRAPHIC_LOCATION,
    {% if  fact_location_function %}
        '{{fact_location_function}}' as location_function,
    {% else %}
        location_function as location_function,
    {% endif %}
    inv.loaded_timestamp,
    business_organisation_number,
    origin_organisation_number,
    true as is_ghost

from {{ ref(stg_of_fact_table) }} inv
    inner join {{ ref('utl_source_organisations') }} src
on inv.source_db_id = src.business_organisation_number
    group by organisation_id,{{location_col_name}},location_function,loaded_timestamp,business_organisation_number,origin_organisation_number
    ) all_fact_data

        {% if is_incremental() %}
        where all_fact_data.loaded_timestamp > (select max(loaded_timestamp) from {{ ref(stg_of_fact_table) }} )
        {% endif %}
{%- endmacro -%}