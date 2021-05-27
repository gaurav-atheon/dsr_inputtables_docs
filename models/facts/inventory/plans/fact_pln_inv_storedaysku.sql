{{
    config(
        materialized='incremental',
        unique_key='unique_key',
        cluster_by=['loaded_timestamp']
    )
}}

select
    ord.day_date,
    src.organisation_id, --converted to dsr id
    loc.location_id, --converted to dsr id
    prd.product_id, --converted to dsr id
    ord.ranged,
    ord.loaded_timestamp,
   '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/London")) }}'  as runstartedtime,
    {{ dbt_utils.surrogate_key(['ord.day_date','src.organisation_id','loc.location_id','prd.product_id']) }} as unique_key

from {{ref('stg_pln_inv_storedaysku')}} ord

inner join {{ ref('utl_source_organisations') }} src --need relationship validation earlier in the flow
on ord.source_db_id = src.business_organisation_number

inner join {{ ref('dim_location') }} loc --need relationship validation earlier in the flow
on loc.organisation_id = src.organisation_id
and loc.organisation_location_id = ord.organisation_location_id
and loc.location_function = 'point of sale'

inner join {{ ref('dim_product') }} prd --need relationship validation earlier in the flow
on prd.organisation_id = src.organisation_id
and prd.organisation_sku = ord.organisation_sku

        {% if is_incremental() %}
        where ord.loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}

union all

    select
    day_date,
    organisation_id, --converted to dsr id
    organisation_location_id_to location_id, --converted to dsr id
    product_id, --converted to dsr id
    status as ranged,
    max(loaded_timestamp),
    '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/London")) }}'  as runstartedtime,
    {{ dbt_utils.surrogate_key(['day_date','organisation_id','location_id','product_id']) }} as unique_key

    from {{ref('fact_pln_mvt_depotstoredaysku')}} pln
    where  status = true and model_version = 'morrisons_ranging'
    {% if is_incremental() %}
        and pln.loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
    {% endif %}
    group by
        day_date,
        organisation_id,
        location_id,
        product_id,
        status,
        runstartedtime,
        unique_key
