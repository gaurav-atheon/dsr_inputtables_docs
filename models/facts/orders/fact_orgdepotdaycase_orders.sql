{{
    config(
        materialized='incremental',
        unique_key='unique_key'
    )
}}

select
    day_date,
    src.organisation_id as organisation_id_to, --converted to DSR ID
    org.organisation_id as organisation_id_from, --converted to DSR ID BUSINESS_ORGANISATION_NUMBER_FROM
    loc.LOCATION_ID, --converted to DSR ID
    prd.logisticitem_ID, --converted to DSR ID
    CASES_ORDERED_IN,
    CASES_MATCHED_IN,
    ord.loaded_timestamp,
    {{ dbt_utils.surrogate_key(['ord.day_date','src.organisation_id','org.organisation_id','loc.LOCATION_ID','prd.logisticitem_ID']) }} as unique_key

from {{ ref('stg_orgdepotdaycase_orders') }} ord

left join {{ ref('utl_source_organisations') }} src --this should really be "inner", with relationship validation earlier in the flow
on ord.source_db_id = src.business_organisation_number

left join {{ ref('dim_organisation_mapping') }} org --this should really be "inner", with relationship validation earlier in the flow
on ord.BUSINESS_ORGANISATION_NUMBER_FROM = org.business_organisation_number
and org.origin_organisation_id = src.organisation_id

left join {{ ref('dim_location') }} loc --this should really be "inner", with relationship validation earlier in the flow
on loc.organisation_ID = src.organisation_ID
and loc.ORGANISATION_LOCATION_ID = ord.ORGANISATION_LOCATION_ID
and loc.location_function = 'Distribution Location'

left join {{ ref('dim_logisticitem') }} prd --this should really be "inner", with relationship validation earlier in the flow
on prd.organisation_ID = src.organisation_ID
and prd.ORGANISATION_case = ord.ORGANISATION_case

        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }})
        {% endif %}