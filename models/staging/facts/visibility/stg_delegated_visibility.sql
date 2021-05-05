{{
    config(
        materialized='table'
    )
}}

with
ranked_data as
(
select
    delegator_origin_organisation_number,
    delegator_organisation_number,
    delegatee_origin_organisation_number,
    delegatee_organisation_number,
    subject_origin_organisation_number,
    subject_organisation_number,
    product_type,
    organisation_product,
    delegated_status,
    loaded_timestamp,
    created_timestamp,
    row_number() over (partition by delegator_origin_organisation_number, delegator_organisation_number,
                                    delegatee_origin_organisation_number, delegatee_organisation_number,
                                    subject_origin_organisation_number, subject_organisation_number,
                                    product_type, organisation_product order by loaded_timestamp desc) rank

from {{ source('dsr_input', 'input_delegated_visibility') }}
)

select *
from ranked_data
where rank = 1