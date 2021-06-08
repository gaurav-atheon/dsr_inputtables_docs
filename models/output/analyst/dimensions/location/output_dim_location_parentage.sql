select
     creator_organisation_id,
     subject_organisation_id,
     subject_location_id,
     parent_organisation_id,
     parent_location_id,
     parentage_id,
    runstartedtime
from {{ ref('dim_location_parentage') }}