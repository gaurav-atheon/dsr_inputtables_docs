select
     creator_organisation_id,
     subject_organisation_id,
     parent_organisation_id,
     parentage_id
from {{ ref('dim_organisation_parentage') }}