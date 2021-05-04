WITH
delegation as
(
select
    {{ dbt_utils.surrogate_key(['delegator_origin_organisation_number','delegator_organisation_number']) }} as delegator_organisation_id,
    {{ dbt_utils.surrogate_key(['delegatee_origin_organisation_number','delegatee_organisation_number']) }} as delegatee_organisation_id,
    {{ dbt_utils.surrogate_key(['subject_origin_organisation_number','subject_organisation_number','product_type']) }} as item_id,
    product_type,
    delegated_status,
    loaded_timestamp,
    created_timestamp
from {{ ref('stg_delegated_visibility') }}
where delegated_status = TRUE
),

delegator_orgs as
(
select d.delegator_organisation_id, o.descendant_organisation_id, d.delegatee_organisation_id,
d.item_id, d.product_type,
d.loaded_timestamp, d.created_timestamp
from delegation d
inner join {{ ref('utl_organisation_descendants') }} o
ON d.delegator_organisation_id = o.organisation_id
),

delegated_days as
(
  SELECT v.table_reference,
         v.product_type,
         v.item_id,
         d.DAY_DATE,
         o.delegatee_organisation_id,
         v.location_function,
         '400 - Delegated' ACCESS_LEVEL
  FROM   {{ ref('int_organisationsku_visibility') }} v
         inner join delegator_orgs o
            on v.SUBJECT_ORGANISATION_ID = o.descendant_organisation_id
            and v.product_type = o.product_type
            and v.item_id = o.item_id
         inner join {{ ref('dim_date') }} d
                 ON d.DAY_DATE BETWEEN v.day_date_from AND v.day_date_to
  GROUP  BY v.table_reference,
            v.product_type,
            v.item_id,
            d.DAY_DATE,
            o.delegatee_organisation_id,
            v.location_function
)

select *
from delegated_days