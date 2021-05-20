SELECT DISTINCT dom.origin_organisation_number, 
                dom.business_organisation_number, 
                business_organisation_name 
FROM   {{ ref('dim_organisation_mapping') }} dom 
       LEFT JOIN {{ ref('utl_master_organisation') }} umo 
              ON dom.origin_organisation_id = umo.organisation_id 
       INNER JOIN {{ ref('dim_organisation_parentage') }} dop 
               ON dop.parent_organisation_id = dom.organisation_id 
       INNER JOIN {{ ref('utl_organisation_parents') }} uop 
               ON uop.parent_organisation_id = dop.parent_organisation_id;