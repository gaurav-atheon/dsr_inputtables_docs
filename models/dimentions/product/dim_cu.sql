select
    {{ dbt_utils.surrogate_key(['DECISION_MAKER_ORIGIN_ORGANISATION_NUMBER','DECISION_MAKER_ORGANISATION_NUMBER','GROUPING_KEY','loaded_timestamp']) }} as consumer_unit_id,
    --attributes
    {{ dbt_utils.surrogate_key(['DECISION_MAKER_ORIGIN_ORGANISATION_NUMBER','DECISION_MAKER_ORGANISATION_NUMBER']) }} as Decision_maker_organisation_ID
from {{ ref('stg_sku_grouping') }}
group by consumer_unit_id,Decision_maker_organisation_ID