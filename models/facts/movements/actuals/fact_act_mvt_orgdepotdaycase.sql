{{
    config(
        materialized='incremental',
        unique_key='unique_key',
        cluster_by=['loaded_timestamp']
    )
}}

select
    day_date,
    src.organisation_id as organisation_id_to, --converted to dsr id
    org.organisation_id as organisation_id_from, --converted to dsr id business_organisation_number_from
    loc.location_id, --converted to dsr id
    prd.logisticitem_id, --converted to dsr id
    cases_ordered_in,
    cases_fulfilled_in,
    ord.loaded_timestamp,
    '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/London")) }}'  as runstartedtime,
    {{ dbt_utils.surrogate_key(['ord.day_date','src.organisation_id','org.organisation_id','loc.location_id','prd.logisticitem_id']) }} as unique_key

from {{ ref('stg_act_mvt_orgdepotdaycase') }} ord

inner join {{ ref('utl_source_organisations') }} src --need relationship validation earlier in the flow
on ord.source_db_id = src.business_organisation_number

inner join {{ ref('dim_organisation_mapping') }} org --need relationship validation earlier in the flow
on ord.business_organisation_number_from = org.business_organisation_number
and org.origin_organisation_id = src.organisation_id

inner join {{ ref('dim_location') }} loc --need relationship validation earlier in the flow
on loc.organisation_id = src.organisation_id
and loc.organisation_location_id = ord.organisation_location_id
and loc.location_function = 'distribution location'

inner join {{ ref('dim_logisticitem') }} prd --need relationship validation earlier in the flow
on prd.organisation_id = src.organisation_id
and prd.organisation_case = ord.organisation_case

        {% if is_incremental() %}
        where ord.loaded_timestamp > nvl((select max(loaded_timestamp) from {{ this }}), to_timestamp('0'))
        {% endif %}