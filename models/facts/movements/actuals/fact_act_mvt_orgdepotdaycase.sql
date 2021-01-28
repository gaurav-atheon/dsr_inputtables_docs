{{
    config(
        materialized='incremental',
        unique_key='fct_act_mvt_orgdepotdaycase_key',
        cluster_by=['loaded_timestamp']
    )
}}

select
    day_date,
    src.organisation_id as organisation_id_to, --converted to DSR ID
    org.organisation_id as organisation_id_from, --converted to DSR ID BUSINESS_ORGANISATION_NUMBER_FROM
    loc.LOCATION_ID, --converted to DSR ID
    prd.logisticitem_ID, --converted to DSR ID
    CASES_ORDERED_IN,
    CASES_FULFILLED_IN,
    ord.loaded_timestamp,
    {{ dbt_utils.surrogate_key(['ord.day_date','src.organisation_id','org.organisation_id','loc.LOCATION_ID','prd.logisticitem_ID']) }} as fct_act_mvt_orgdepotdaycase_key

from {{ ref('stg_act_mvt_orgdepotdaycase') }} ord

inner join {{ ref('utl_source_organisations') }} src --need relationship validation earlier in the flow
on ord.source_db_id = src.business_organisation_number

inner join {{ ref('dim_organisation_mapping') }} org --need relationship validation earlier in the flow
on ord.BUSINESS_ORGANISATION_NUMBER_FROM = org.business_organisation_number
and org.origin_organisation_id = src.organisation_id

inner join {{ ref('dim_location') }} loc --need relationship validation earlier in the flow
on loc.organisation_ID = src.organisation_ID
and loc.ORGANISATION_LOCATION_ID = ord.ORGANISATION_LOCATION_ID
and loc.location_function = 'Distribution Location'

inner join {{ ref('dim_logisticitem') }} prd --need relationship validation earlier in the flow
on prd.organisation_ID = src.organisation_ID
and prd.ORGANISATION_case = ord.ORGANISATION_case

        {% if is_incremental() %}
        where ord.loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}