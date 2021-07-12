{% docs dim_cu%}

**Description**

Contains distinct DSR Consumer Units. A consumer unit is defined as an organisationally defined group of SKUs. This means that there could theoretically be overlapping or duplicate "real world" consumer units; consumer units are scoped to the creator organisation.

Model Operation :- Incrementally adds data.

**Grain**
One record per consumer unit along with the creator organization ID.

**Purpose**
Currently this is simply a distinct list of all DSR Consumer Units, created for completeness of table structure. Attributes will be added in future when a design has been agreed.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs dim_date%}

**Description**

Contains distinct list of all days with standardised attributes such as month, month_name, date_of_week etc. 

Model Operation :- Incrementally adds data.

**Grain**
One record per day_date in YYYY-MM-DD format.

**Purpose**
The Day_date describes the days that facts occur on.


**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs dim_location%}

**Description**

Contains details such as longitude, latitudes etc for all physical entities (depot, store, etc) .It also contains surrogate key(hash key) for every unique record and unions the ghost records which are only present in fact but are yet to provided to dimension which are stored with "Is_ghost" set to "True"

Model Operation :- Incrementally adds data.

**Grain**
One record per physical location describing its immutable attributes as stated by the organisation

**Purpose**
To store attributes of the location in one place and have a large number of attributes assigned to them. We add organisation to allow multiple "opinions" to be accepted (and grouped later)

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs dim_location_group%}

**Description**
Contains groups of location defined on certain attributes and with a group name along with value

Model Operation :- Incrementally adds data.

**Grain**
One record per location group id for creator organisation, subject organisation, location function and group name

**Purpose**
Groups can be defined for locations which can share similar attributes.
Creator such as atheon can create its own group with a unique group name and assign a group value to all group locations

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs dim_location_parentage%}

**Description**

Contains parentage for locations. It defines parents location for a subject organisation created by the creator organisation.

Model Operation :- Incrementally adds data.

**Grain**
One record defines parent location for a subject location.

**Purpose**
Used to define parentage for physical location. The parentage can be defined by creator organisation for a subject organisation which will allow multiple parentage for same location and priority can be given on the basis of creator org ID while retrieving the information

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs dim_logisticitem%}

**Description**

Contains distinct cases. A logisticitem is defined as an organisationally defined case. Logistic item are scoped to the creator organisation.It also contains surrogate key(hash key) for every unique record and unions the ghost records which are only present in fact but are yet to provided to dimension which are stored with "Is_ghost" set to "True"

Model Operation :- Incrementally adds data.

**Grain**
One record per case along with the creator organization ID.

**Purpose**
Used to distinctly identify and store cases with respect to creator organisation

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs dim_logisticitem_mapping%}

**Description**


**Grain**


**Purpose**


**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs dim_organisation%}

**Description**

Contains list of all organization. Creator can defines attributes for an organisation.It also contains surrogate key(hash key) for every unique record and unions the ghost records which are only present in fact but are yet to provided to dimension which are stored with "Is_ghost" set to "True"

Model Operation :- Incrementally adds data

**Grain**
One record per organization.

**Purpose**
Used to distinctly identify and store organisation.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs dim_organisation_mapping%}

**Description**

Contains distinct organization. A unique organisation Id is defined by the organisation number and Id of the organisation which creates it.

Model Operation :- Incrementally adds data

**Grain**
One record per organization.

**Purpose**
Used to define mapping for the organisation. It allows a same organisation differentiated on the basis of the creator organisation.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs dim_organisation_parentage%}

**Description**

Contains list of organisation and defines the parent organisation for them which can be entered by a creator organisation

Model Operation :- Incrementally adds data

**Grain**
One record per organization per creator organisation

**Purpose**
Used to define parentage for an organisation. A creator organsaition can define the parentage for any organsaition.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs dim_org_calendar%}

**Description**
Contains distinct list of days which is defined for specific organisation

Model Operation :- Incrementally adds data.

**Grain**
One records per day_date in YYYY-MM-DD format.

**Purpose**
The Day_date describes the days that facts occur on.


**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs dim_product%}

**Description**

Contains distinct sku. A product_ID is defined as an organisationally defined sku. product id are scoped to the creator organisation.It also contains surrogate key(hash key) for every unique record and unions the ghost records which are only present in fact but are yet to provided to dimension which are stored with "Is_ghost" set to "True"

Model Operation :- Incrementally adds data.

**Grain**
One record per sku along with the creator organization ID.

**Purpose**
Used to distinctly identify and store sku with respect to creator organisation

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs dim_product_group%}

**Description**

Contains groups of sku defined on certain attributes and with a group name along with value. This means there could be multiple sku which share similar products attribute and an creator organisation can match them together.

Model Operation :- Incrementally adds data.

**Grain**
One record per sku id for creator organisation, subject organisation and group name

**Purpose**
Groups can be defined for sku which can share similar attributes.
Creator such as atheon can create its own group with a unique group name and assign a group value to all group sku

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs dim_product_mapping%}

**Description**


**Grain**


**Purpose**


**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs dim_promotion%}

**Description**

Contains distinct promotion defined for an organisation. A promotion is defined as an organisationally defined promotion with a promotion number , start and end date. Promotions are scoped to the creator organisation for a subject organisation.

Model Operation :- Delete the old promotion(If present for all sku) and new data for a promotion. 

**Grain**
One record per promotion per subject organisation per sku.

**Purpose**
Used to distinctly identify and store promotion details for an organisation. It details all the attributes for promotion like start date, end date, promotion number and sku.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs dim_tu%}

**Description**

Contains distinct DSR Traded Units. A traded unit is defined as an organisationally defined group of cases. This means that there could theoretically be overlapping or duplicate "real world" traded units; traded units are scoped to the creator organisation.

Model Operation :- Incrementally adds data.

**Grain**
One record per traded unit along with the creator organization ID.

**Purpose**
Currently this is simply a distinct list of all DSR traded Units, created for completeness of table structure. Attributes will be added in future when a design has been agreed.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs fact_act_inv_locationdaycase%}

**Description**

Fact Type: Actual

Data Representation: Inventory

Contents: Contains actual inventory recorded at a location on a day at case level. This is measured at a point in time defined by the retailer to be representative of that day.
The table contains both depot and store inventory information, but only where case level information is available. Data is NOT derived backwards from SKU level inventory.

Model Operation: Incrementally adds new data. Joins dimensions based on business keys to ensure referential integrity and to retrieve and store DSR table ID's.

**Grain**
One record per location, per day, per case. 

Records only where source data submitted, therefore there may be "zero" inventory records, but the data is not densified for NULL records.

**Purpose**
Used to provide stock information at specific locations over time. This data can be aggregated to the SKU level by joining the logistic item to provide a consistent stock picture where stock measures are inconsistent

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs fact_act_inv_locationdaysku%}

**Description**

Fact Type: Actual

Data Representation: Inventory

Contents: Contains actual inventory recorded at a location on a day at sku level. This is measured at a point in time defined by the retailer to be representative of that day.
The table contains both depot and store inventory information, but only where sku level information is available.

Model Operation: Incrementally adds new data. Joins dimensions based on business keys to ensure referential integrity and to retrieve and store DSR table ID's.


**Grain**
One record per location, per day, per sku. 

Records only where source data submitted, therefore there may be "zero" inventory records, but the data is not densified for NULL records.

**Purpose**
Used to provide stock information at specific locations over time. The data is aggregated to the SKU level by joining the logistic item with this table to provide a consistent stock picture where stock measures are inconsistent.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs fact_act_mvt_depotstoredaysku%}

**Description**

Fact Type: Actual

Data Representation: Movement

Contents: Contains actual movement recorded between a depot and a store on a day at sku level. This is measured at a point in time defined by the retailer to be representative of that day.

Model Operation: Incrementally adds new data. Joins dimensions based on business keys to ensure referential integrity and to retrieve and store DSR table ID's.

**Grain**
One record per sku movement per organisation_location_id_from to organisation_location_id_to per day.

**Purpose**
Used to provide movement information of a sku from depot to store for a day. The data is aggregated to provide information on actual units ordered by a store and unites fulfilled by the depot.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs fact_act_mvt_orgdepotdaycase%}

**Description**

Fact Type: Actual

Data Representation: Movement

Contents: Contains actual movement recorded between a organisation\supplier and a depot on a day at case level. This is measured at a point in time defined by the retailer to be representative of that day.

Model Operation: Incrementally adds new data. Joins dimensions based on business keys to ensure referential integrity and to retrieve and store DSR table ID's.

**Grain**
One record per case movement per supplier to depot per day.

**Purpose**
Used to provide movement information of a case from supplier organisation to depot for a day. The data is aggregated to provide information on actual case ordered by a retailer and unites fulfilled by the supplier to retailers depot.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs fact_act_mvt_orgdepotdaysku%}

**Description**

Fact Type: Actual

Data Representation: Movement

Contents: Contains actual movement recorded between a organisation\supplier and a depot on a day at sku level. This is measured at a point in time defined by the retailer to be representative of that day.

Model Operation: Incrementally adds new data. Joins dimensions based on business keys to ensure referential integrity and to retrieve and store DSR table ID's.

**Grain**
One record per sku movement per supplier to depot per day.

**Purpose**
Used to provide movement information of a sku from supplier organisation to depot for a day. The data is aggregated to provide information on actual sku ordered by a retailer and unites fulfilled by the supplier to retailers depot.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs fact_act_mvt_storedaysku%}

**Description**

Fact Type: Actual

Data Representation: Movement

Contents: Contains actual movement recorded for a store at sku level. This is measured at a point in time defined by the retailer to be representative of that day.

Model Operation: Incrementally adds new data. Joins dimensions based on business keys to ensure referential integrity and to retrieve and store DSR table ID's.

**Grain**
One record per store, per day, per sku. 


**Purpose**
Used to provide movement information for a SKU at specific store over time. <<Epos>> data to enter

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs fact_pln_inv_storedaysku%}

**Description**

Fact Type: Plan

Data Representation: Inventory

Contents: Contains planned inventory recorded for a store at sku level. This is measured at a point in time defined by the retailer to be representative of that day.

Model Operation: Incrementally adds new data. Joins dimensions based on business keys to ensure referential integrity and to retrieve and store DSR table ID's.

**Grain**
One record per store, per day, per sku.

**Purpose**
Used to provide ranging information for a sku to a store for a day.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs fact_pln_mvt_daysku%}

**Description**


**Grain**


**Purpose**


**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs fact_pln_mvt_depotstoredaysku%}

**Description**

Fact Type: Plan

Data Representation: Movement

Contents: Contains plan movement recorded between a depot and a store on a day at sku level. This is measured at a point in time defined by the retailer to be representative of that day.

Model Operation: Incrementally adds new data. Joins dimensions based on business keys to ensure referential integrity and to retrieve and store DSR table ID's.

**Grain**
One record per sku movement per organisation_location_id_from to organisation_location_id_to per day.

**Purpose**
Used to provide planned movement information of a sku from depot to store for a day. The data is aggregated to provide information on units that is planned by store to order.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs fact_pln_mvt_orgdepotdaysku%}

**Description**

Fact Type: Plan

Data Representation: Movement

Contents: Contains planned movement recorded between a organisation\supplier and a depot on a day at sku level. This is measured at a point in time defined by the retailer(Identified by creator organisation ID) to be representative of that day.

Model Operation: Incrementally adds new data. Joins dimensions based on business keys to ensure referential integrity and to retrieve and store DSR table ID's.

**Grain**
One record per case movement per supplier to depot per day.

**Purpose**
Used to provide planned movement information of a sku from supplier organisation to depot for a day. The data is aggregated to provide information on planned sku ordered by a retailer and unites fulfilled by the supplier to retailers depot.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs fact_pln_mvt_storedaysku%}

**Description**

Fact Type: Plan

Data Representation: Movement

Contents: Contains planned movement recorded between a organisation\supplier and a depot on a day at sku level. This is measured at a point in time defined by the organization(Identified by creator organisation ID) to be representative of that day.

Model Operation: Incrementally adds new data. Joins dimensions based on business keys to ensure referential integrity and to retrieve and store DSR table ID's.

**Grain**
One record per case movement per supplier to depot per day.

**Purpose**
Used to provide planned movement information of a sku from supplier organisation to depot for a day. The data is aggregated to provide information on planned sku ordered by a retailer and unites fulfilled by the supplier to retailers depot

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs int_allfacts_visibility%}

**Description**

Contents: Explicit and Implicit visibilty records for all fact tables, at the individual day, SKU level.

Model Operation: Incrementally creates Explicit and Implicit visibilty records for all fact tables that have new data. Contains initial configuration that allows for appropriate record creation for every fact.

**Grain**
One record per day, per organisation, per item.

**Purpose**
Creates the foundational "event" records to enable a slowly changing dimension to be created.

**Created On**
07/07/2021

**Created By**
Alex Paterson

**Important**


{% enddocs %}
{% docs int_all_ghost_location%}

**Description**

Model Type: intermediary

Contents: Contains the dimension data which is not yet provided by the retailer to the dimension tables. This data will be integrated in the dimension table with is_ghost flag as "true" to differentiate from the actual dimension data from retailer. 

Model Operation: Incrementally adds new data. 

**Grain**
One record per location per organisation per location function

**Purpose**
Used to ensure that we do not lose any fact data in case when the corresponding dimension to not yet supplied by the retailer.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs int_all_ghost_logisticitem%}

**Description**

Model Type: intermediary

Contents: Contains the dimension data which is not yet provided by the retailer to the dimension tables. This data will be integrated in the dimension table with is_ghost flag as "true" to differentiate from the actual dimension data from retailer. 

Model Operation: Incrementally adds new data. 

**Grain**
One record per logistiem(case ID) per organisation

**Purpose**
Used to ensure that we do not lose any fact data in case when the corresponding dimension to not yet supplied by the retailer.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs int_all_ghost_organisation%}

**Description**

Model Type: intermediary

Contents: Contains the dimension data which is not yet provided by the retailer to the dimension tables. This data will be integrated in the dimension table with is_ghost flag as "true" to differentiate from the actual dimension data from retailer. 

Model Operation: Incrementally adds new data. 

**Grain**
One record per organisation.

**Purpose**
Used to ensure that we do not lose any fact data in case when the corresponding dimension to not yet supplied by the retailer.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs int_all_ghost_product%}

**Description**

Model Type: intermediary

Contents: Contains the dimension data which is not yet provided by the retailer to the dimension tables. This data will be integrated in the dimension table with is_ghost flag as "true" to differentiate from the actual dimension data from retailer. 

Model Operation: Incrementally adds new data. 

**Grain**
One record per product id (sku) per organisation

**Purpose**
Used to ensure that we do not lose any fact data in case when the corresponding dimension to not yet supplied by the retailer.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs int_delegated_visibility_scd%}

**Description**

Contents: Visibility records that have been delegated from one organisation to another.

Model Operation: Uses a macro to build a slowly changing dimension from individual dates.

**Grain**
One recrod per fact table, per item, per date period, per organisation and optionally per location function.

**Purpose**
Conforms delegated data to the final visibility table structure.

**Created On**
07/07/2021

**Created By**
Alex Paterson

**Important**


{% enddocs %}
{% docs int_organisationsku_visibility%}

**Description**

Contents: Slowly changing dimension visibility records for 1. Explicit access, 2. Implicit access and 3. Acquired access.

Model Operation: Unions visibility access types 1-2 with visibility access type 3 (acquired visibility). Fully materialises as a table.

**Grain**
One recrod per fact table, per item, per date period, per organisation and optionally per location function.

**Purpose**
Unions the access levels together so they reside in one table.

**Created On**
07/07/2021

**Created By**
Alex Paterson

**Important**


{% enddocs %}
{% docs master_organisation%}

**Description**

Model Type: Seed

Contents: Contains the master organisation of the DSR which in our case is 'Atheon'. This organisation will have no origin organisation number.

Model Operation: The data is loaded just once on special cases

**Grain**
One record per organisation.

**Purpose**
Used to launch the very first organisation to DSR system which is called master organisation. This organisation will create new organization\retailer.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs seed_organisation_type%}

**Description**

Model Type: Seed

Contents: Contains the organisation data and defines if they are 'retailer' or 'supplier'. E.g. it will contain 'morrisons' and defines it as 'retailer'. 

Model Operation: The data is loaded just once on special cases

**Grain**
One record per organisation.

**Purpose**
Used to identify the organisation type. It can be joined with other models and used to segregate data on the basis of organisation type.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_act_inv_locationdaycase%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per location, per day, per case. 



**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_act_inv_locationdaysku%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per location, per day, per sku. 

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_act_mvt_depotstoredaysku%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per sku movement per depot location id to store id per day.

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_act_mvt_orgdepotdaycase%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per case movement per supplier to depot per day.

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_act_mvt_storedaysku%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per store, per day, per sku. 


**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_calendar%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**


**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_case%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per consumer unit along with the creator organization ID

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_case_grouping%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**


**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_date%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One records per day_date in YYYY-MM-DD format.

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_delegated_visibility%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**


**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Alex Paterson

**Important**


{% enddocs %}
{% docs stg_location%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per physical location describing its immutable attributes.

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_location_group%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per location group id for creator organisation, subject organisation, location function and group name

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_location_parentage%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record defines parent location for a subject location.

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_organisation%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per sku along with the creator organization number

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_organisation_mapping%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per organization.

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_organisation_parentage%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per organization per creator organisation

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_pln_inv_storedaysku%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per store, per day, per sku.

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_pln_mvt_daysku%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**


**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_pln_mvt_depotstoredaysku%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per sku movement per organisation_location_id_from to organisation_location_id_to per day.

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_pln_mvt_orgdepotdaysku%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per case movement per supplier to depot per day.

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_pln_mvt_storedaysku%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per case movement per supplier to depot per day.

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_promotion%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per promotion per subject organisation per sku.

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_sku%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per sku along with the creator organization number

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs stg_sku_grouping%}

**Description**

Model Type: Staging

Contents: Contains the data coming in from input tables , eliminating duplicates and attaches uniques key to it

Model Operation: Incrementally adds new data. 

**Grain**
One record per sku id for creator organisation, subject organisation and group name

**Purpose**
Used to remove any duplicate data from input table and maintains store the input data locally to DSR for future use.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs utl_organisationsku_visibility%}

**Description**

Contents: Contains a record for every SKU that an organisation has gained the right to be visible to them. This visibility is date bound and scoped to each fact table. 
Visibility can be acquired though:
1. Explicit access - Organisation is the creator or source of the data - reserved for source databases
2. Implicit access - Organisation is a party to a transaction
3. Acquired access - Entitlement acquired through a “visibility process” for metrics downstream of metrics implicitly given
4. Delegated access - Delegation from one of the above access levels to another organisation

Model Operation: Unions visibility access types 1-3 with visibility access type 4 (delegated visibility)

**Grain**
One recrod per fact table, per item, per date period, per organisation and optionally per location function.

**Purpose**
Join directly to the appropriate fact table to provide pre-calculated set of products for which the organisation has rights to see

**Created On**
07/07/2021

**Created By**
Alex Paterson

**Important**


{% enddocs %}
{% docs utl_orggroupsku_visibility%}

**Description**

Contents: Contains a record for every SKU that an organisation group has gained the right to be visible to them. This visibility is date bound and scoped to each fact table. 
Visibility can be acquired though:
1. Explicit access - Organisation is the creator or source of the data - reserved for source databases
2. Implicit access - Organisation is a party to a transaction
3. Acquired access - Entitlement acquired through a “visibility process” for metrics downstream of metrics implicitly given
4. Delegated access - Delegation from one of the above access levels to another organisation

Model Operation: Takes visibility at the organisation level and rolls it up to "organisation group" level; i.e. the top of the organisational hierarchy.

**Grain**
One recrod per fact table, per item, per date period, per organisation group and optionally per location function.

**Purpose**
Join directly to the appropriate fact table to provide pre-calculated set of products for which the organisation group has rights to see

**Created On**
07/07/2021

**Created By**
Alex Paterson

**Important**


{% enddocs %}
{% docs utl_location_hierarchy%}

**Description**

Contents: Contains a record for every location and defines the parentage hierarchy. It will consolidates the parent child relationships of location,returning the top of the hierarchical tree for each location. This allows attribution of metrics to a single, top level location from all the children below.

Model Operation: Takes records at the location level and rolls it up to "location group" level; i.e. the top of the location hierarchy.

**Grain**
One record per location per location group

**Purpose**
Used to identify the parentage hierarchy of a location which can be used to identify if an location is used by different supplier\retailer by linking to same parent location id.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs utl_location_parents%}

**Description**

Contents: Contains a record for every location and defines the parent for it. It identifies the parent location for every location(if none present than the location is parent itself).

Model Operation: Takes records at the location level and defines the parent for it

**Grain**
One record per location.

**Purpose**
Used to identify the parent of any location.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs utl_master_organisation%}

**Description**

Contents: Contains a record for master organisation which create any new retailer in DSR. It should be the first organisation in DSR universe

Model Operation: It gets the data from seed_organisation which is executed only when a new\update to master organsation is recieved

**Grain**
One record per organisation.

**Purpose**
Used to identify the Master organisation of DSR which initiates the DSR.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs utl_organisation_descendants%}

**Description**

Contents: Contains a record for every organsiation and defines the child hierarchy. It will consolidates the parent child relationships of organisations,returning the lowest child of the hierarchical tree for each organisation. 

Model Operation: Takes records at the organsaition level and rolls it down to level; i.e. the lowest child of the organsaition hierarchy.

**Grain**
One record per organsaition per organsaition group

**Purpose**
Used to identify the decendants hierarchy of a organsaition which are created by atheon. Used to identify childern of any organsaition organisation 

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs utl_organisation_hierarchy%}

**Description**

Contents: Contains a record for every organsiation and defines the parentage hierarchy. It will consolidates the parent child relationships of organisations,returning the top of the hierarchical tree for each organisation. This allows attribution of metrics to a single, top level organisation from all the children below.

Model Operation: Takes records at the organsaition level and rolls it up to "organsaition group" level; i.e. the top of the organsaition hierarchy.

**Grain**
One record per organsaition per organsaition group

**Purpose**
Used to identify the parentage hierarchy of a organsaition which are created by atheon.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs utl_organisation_parents%}

**Description**

Contents: Contains a record for every organisation and defines the parent for it. It identifies the parent for every organisation(if none present than the location is parent itself).

Model Operation: Takes records at the organisation level and defines the parent for it

**Grain**
One record per organisation.

**Purpose**
Used to identify the parent of any organisation.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
{% docs utl_retailer_organisations%}

**Description**

Content:
This utility provides all organisations that belong to the master organisation and also are the parent to a retailer organisation.
It essentially returns a list of all organisations that have a confirmed supplier relationship with each retailer.

model Operation:

**Grain**
One record per organisation.

**Purpose**


**Created On**
07/07/2021

**Created By**
Alex Paterson

**Important**


{% enddocs %}
{% docs utl_source_organisations%}

**Description**

Contents: Contains a record for organisation which are created by master organisation(in most cases Atheon). This are usually the retailers that are created by Atheon

Model Operation: It generates a view using data from organisation and mapping organsation dimetion. 

**Grain**
One record per organisation.

**Purpose**
As only source organisation can send\create data in DSR, this view will provide comprehensive list of all source organsition which can provide\create data in DSR. 

**Created On**
07/07/2021

**Created By**
Gaurav Jain

**Important**


{% enddocs %}
