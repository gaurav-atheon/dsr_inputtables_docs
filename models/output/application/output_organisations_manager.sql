WITH retailer_source -- get all retailer organisation_ids  
     AS (SELECT organisation_id, organisation_name
         FROM   {{ ref('dim_organisation') }} 
         WHERE  organisation_type = 'retailer'), 
     retailer_organisations 
     -- display organisations where the origin of the organisation is the retailer  
     AS (SELECT r.organisation_id AS retailer_id,
                r.organisation_name,
                om.organisation_id, 
                om.business_organisation_name, 
                om.business_organisation_number 
         FROM   {{ ref('dim_organisation_mapping') }} om 
                INNER JOIN retailer_source r 
                        ON om.origin_organisation_id = r.organisation_id), 
     organisation_parentage 
     -- find the parent organisation_id for all retailer organisations and display that  
     AS (SELECT ro.*, 
                op.parent_organisation_id 
         FROM   retailer_organisations ro 
                LEFT JOIN {{ ref('dim_organisation_parentage') }} op 
                       -- check to see if organisation has a parent 
                       ON op.subject_organisation_id = ro.organisation_id), 
     check_parent 
     AS (SELECT op.*, 
                om.origin_organisation_id       AS origin_of_parent_id, 
                origin_organisation_number      AS origin_of_parent_number, 
                om.business_organisation_number AS host_name 
         FROM   organisation_parentage op 
                LEFT JOIN {{ ref('dim_organisation_mapping') }} om 
                       ON op.parent_organisation_id = om.organisation_id) 
SELECT lower(organisation_name) as retailer, business_organisation_number, business_organisation_name
FROM   check_parent 
WHERE  origin_of_parent_number = '1' 
        OR origin_of_parent_number IS NULL 
-- if origin of parent is null or Atheon then display