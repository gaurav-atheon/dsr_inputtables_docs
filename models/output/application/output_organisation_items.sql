SELECT DISTINCT
LOWER(retailer_org.organisation_name) AS retailername,
organisation_sku AS retailersku,
description AS retailerproductdescription,
to_varchar(prd.attributes:class_name) AS retailerproductcategory,
to_varchar(prd.attributes:subclass_name) AS retailerproductsubcategory,
org.organisation_name AS suppliername
FROM utl_orggroupsku_visibility osv 
INNER JOIN dim_product prd ON prd.product_id = osv.item_id
INNER JOIN dim_organisation org ON org.organisation_id = osv.organisation_group_id
INNER JOIN dim_organisation retailer_org ON retailer_org.organisation_id = prd.ORGANISATION_ID;