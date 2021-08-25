

{% docs input_act_inv_locationdaycase%}

**Description**
Data category: Actual

Data Representation: Inventory

Contents: Contains actual inventory recorded at a location on a day at case level. This is measured at a point in time defined by the retailer to be representative of that day.
The table contains both depot and store inventory information, but only where case level information is available.

**Grain**
One record per location, per day, per case. 

Records only where source data submitted, therefore there may be "zero" inventory records, but the data is not densified for NULL records.

**Purpose**
Used to provide stock information at specific locations over time. This data can be aggregated to the SKU level by joining the logistic item to provide a consistent stock picture where stock measures are inconsistent

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}








{% docs input_act_inv_locationdaysku%}

**Description**
Data Category: Actual

Data Representation: Inventory

Contents: Contains actual inventory recorded at a location on a day at sku level. This is measured at a point in time defined by the retailer to be representative of that day.
The table contains both depot and store inventory information, but only where sku level information is available.


**Grain**
One record per location, per day, per sku. 

Records only where source data submitted, therefore there may be "zero" inventory records, but the data is not densified for NULL records.

**Purpose**
Used to provide stock information at specific locations over time. The data is aggregated to the SKU level by joining the logistic item with this table to provide a consistent stock picture where stock measures are inconsistent.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}








{% docs input_act_mvt_depotstoredaysku%}

**Description**

Data Category: Actual

Data Representation: Movement

Contents: Contains actual movement recorded between a depot and a store on a day at sku level. This is measured at a point in time defined by the retailer to be representative of that day.

**Grain**
One record per sku movement per organisation location from to organisation location id to per day.

**Purpose**
Used to provide movement information of a sku from depot to store for a day. The data is aggregated to provide information on actual units ordered by a store and unites fulfilled by the depot.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}








{% docs input_act_mvt_orgdepotdaycase%}

**Description**

Data Category: Actual

Data Representation: Movement

Contents: Contains actual movement recorded between a organisation\supplier and a depot on a day at case level. This is measured at a point in time defined by the retailer to be representative of that day.

**Grain**
One record per case movement per supplier to depot per day.

**Purpose**
Used to provide movement information of a case from supplier organisation to depot for a day. The data is aggregated to provide information on actual case ordered by a retailer and unites fulfilled by the supplier to retailers depot.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}








{% docs input_act_mvt_orgdepotdayordercase%}

**Description**

Data Category: Actual

Data Representation: Movement

Contents: Contains actual movement recorded between a organisation\supplier and a depot on a day at case level for every order. This is measured at a point in time defined by the retailer to be representative of that day.

**Grain**
One record per case movement per supplier to depot per day.

**Purpose**
Used to provide movement information of a case from supplier organisation to depot for a day per order. The data is aggregated to provide information on actual case ordered by a retailer and unites fulfilled by the supplier to retailers depot.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}









{% docs input_act_mvt_storedaysku%}

**Description**

Data Category: Actual

Data Representation: Movement

Contents: Contains actual movement recorded for a store at sku level. This is measured at a point in time defined by the retailer to be representative of that day.

**Grain**
One record per store, per day, per sku. 

**Purpose**
Used to provide movement information for a SKU at specific store over time. 

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}














{% docs input_calendar%}

**Description**
Contains distinct list of days which is defined for specific organisation

**Grain**
One records per day_date in YYYY-MM-DD format.

**Purpose**
The Day_date describes the days that facts occur on.


**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}






{% docs input_case%}

**Description**
Contains distinct cases. It contains organisationally defined casewhich is scoped to the creator organisation.

**Grain**
One record per case along with the creator organization ID.

**Purpose**
Used to distinctly identify and store cases with respect to creator organisation

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}







{% docs input_case_grouping%}

**Description**


**Grain**


**Purpose**


**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}







{% docs input_date%}

**Description**
Contains distinct list of all days with standardised attributes such as month, month_name, date_of_week etc. 

**Grain**
One record per day_date in YYYY-MM-DD format.

**Purpose**
The Day_date describes the days that facts occur on


**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}









{% docs input_delegated_visibility%}

**Description**


**Grain**


**Purpose**


**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}










{% docs input_location%}

**Description**

Contains details such as longitude, latitudes etc for all physical entities (depot, store, etc) .

**Grain**
One record per physical location describing its immutable attributes as stated by the organisation

**Purpose**
To store attributes of the location in one place and have a large number of attributes assigned to them. We add organisation to allow multiple "opinions" to be accepted (and grouped later)

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}







{% docs input_location_group%}

**Description**
Contains groups of location defined on certain attributes and with a group name along with value

**Grain**
One record per location group id for creator organisation, subject organisation, location function and group name

**Purpose**
Groups can be defined for locations which can share similar attributes.
Creator such as atheon can create its own group with a unique group name and assign a group value to all group locations

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}











{% docs input_location_parentage%}

**Description**
Contains parentage for locations. It defines parents location for a subject organisation created by the creator organisation.

**Grain**
One record defines parent location for a subject location.

**Purpose**
Used to define parentage for physical location. The parentage can be defined by creator organisation for a subject organisation which will allow multiple parentage for same location and priority can be given on the basis of creator org ID while retrieving the information

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}











{% docs input_organisation%}

**Description**
Contains list of all organization. Creator can defines attributes for an organisation.I

**Grain**
One record per organization.

**Purpose**
Used to distinctly identify and store organisation.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}




{% docs input_organisation_mapping%}

**Description**
Contains distinct organization. A unique organisation Id is defined by the organisation number and Id of the organisation which creates it.

**Grain**
One record per organization.

**Purpose**
Used to define mapping for the organisation. It allows a same organisation differentiated on the basis of the creator organisation.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}




{% docs input_organisation_parentage%}

**Description**

Contains list of organisation and defines the parent organisation for them which can be entered by a creator organisation


**Grain**
One record per organization per creator organisation

**Purpose**
Used to define parentage for an organisation. A creator organsaition can define the parentage for any organsaition.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}







{% docs input_pln_inv_storedaysku%}

**Description**

Data Category: Plan

Data Representation: Inventory

Contents: Contains planned inventory recorded for a store at sku level. This is measured at a point in time defined by the retailer to be representative of that day.


**Grain**
One record per store, per day, per sku.

**Purpose**
Used to provide ranging information for a sku to a store for a day.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}






{% docs input_pln_mvt_daysku%}

**Description**


**Grain**


**Purpose**


**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}













{% docs input_pln_mvt_depotstoredaysku%}

**Description**

Data Category: Plan

Data Representation: Movement

Contents: Contains plan movement recorded between a depot and a store on a day at sku level. This is measured at a point in time defined by the retailer to be representative of that day.


**Grain**
One record per sku movement per organisation location from to organisation location id to per day.

**Purpose**
Used to provide planned movement information of a sku from depot to store for a day. 

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}















{% docs input_pln_mvt_orgdepotdaysku%}

**Description**

Data Category: Plan

Data Representation: Movement

Contents: Contains planned movement recorded between a organisation\supplier and a depot on a day at sku level. This is measured at a point in time defined by the retailer(Identified by creator organisation ID) to be representative of that day.

**Grain**
One record per case movement per supplier to depot per day.

**Purpose**
Used to provide planned movement information of a sku from supplier organisation to depot for a day. 

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}



















{% docs input_pln_mvt_orgstoredaysku%}

**Description**

Data Category: Plan

Data Representation: Movement

Contents: Contains planned movement recorded between a organisation\supplier and a store on a day at sku level. This is measured at a point in time defined by the retailer(Identified by creator organisation ID) to be representative of that day.

**Grain**
One record per case movement per supplier to store per day.

**Purpose**
Used to provide planned movement information of a sku from supplier organisation to store for a day. 

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}

















{% docs input_pln_mvt_storedaysku%}

**Description**

Data Category: Plan

Data Representation: Movement

Contents: Contains planned movement recorded for a store on a day at sku level. This is measured at a point in time defined by the organization(Identified by creator organisation ID) to be representative of that day.


**Grain**
One record per sku per store per day.

**Purpose**
Used to provide planned movement information of a sku of a store for a day. 

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}














{% docs input_promotion%}

**Description**

Contains distinct promotion defined for an organisation. A promotion is defined as an organisationally defined promotion with a promotion number , start and end date. Promotions are scoped to the creator organisation for a subject organisation.


**Grain**
One record per promotion per subject organisation per sku.

**Purpose**
Used to distinctly identify and store promotion details for an organisation. It details all the attributes for promotion like start date, end date, promotion number and sku.

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}










{% docs input_sku%}

**Description**

Contains distinct sku. A product sku are organisationally defined and are scoped to the creator organisation.
Model Operation :- Incrementally adds data.

**Grain**
One record per sku along with the creator organization ID.

**Purpose**
Used to distinctly identify and store sku with respect to creator organisation

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}











{% docs input_sku_grouping%}

**Description**

Contains groups of sku defined on certain attributes and with a group name along with value. This means there could be multiple sku which share similar products attribute and an creator organisation can match them together.


**Grain**
One record per sku id for creator organisation, subject organisation and group name

**Purpose**
Groups can be defined for sku which can share similar attributes.
Creator such as atheon can create its own group with a unique group name and assign a group value to all group sku

**Created On**
07/07/2021

**Created By**
Gaurav Jain

{% enddocs %}
