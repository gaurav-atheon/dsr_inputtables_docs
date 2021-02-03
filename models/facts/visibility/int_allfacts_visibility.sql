{{
    config(
        materialized='incremental',
        unique_key='unique_key'
    )
}}
select
day_date,
organisation_id,
item_id,
table_reference,
access_level,
loaded_timestamp,
{{ dbt_utils.surrogate_key(['day_date','organisation_id','item_id','table_reference','access_level']) }} as unique_key
from
(
select day_date, organisation_id, item_id, table_reference, access_level, loaded_timestamp
from {{ ref('int_act_mvt_storedaysku_visibility') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }} where table_reference = 'fact_act_mvt_storedaysku' )
        {% endif %}

union all

select day_date, organisation_id, item_id, table_reference, access_level, loaded_timestamp
from {{ ref('int_act_mvt_orgdepotdaycase_visibility') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }} where table_reference = 'fact_act_mvt_orgdepotdaycase' )
        {% endif %}

union all

select day_date, organisation_id, item_id, table_reference, access_level, loaded_timestamp
from {{ ref('int_act_mvt_depotstoredaysku_visibility') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }} where table_reference = 'fact_act_mvt_depotstoredaysku' )
        {% endif %}

union all

select day_date, organisation_id, item_id, table_reference, access_level, loaded_timestamp
from {{ ref('int_act_inv_locationdaycase_visibility') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }} where table_reference = 'fact_act_inv_locationdaycase' )
        {% endif %}

union all

select day_date, organisation_id, item_id, table_reference, access_level, loaded_timestamp
from {{ ref('int_act_inv_locationdaysku_visibility') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }} where table_reference = 'fact_act_inv_locationdaysku' )
        {% endif %}

union all

select day_date, organisation_id, item_id, table_reference, access_level, loaded_timestamp
from {{ ref('int_pln_inv_storedaysku_visibility') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }} where table_reference = 'fact_pln_inv_storedaysku' )
        {% endif %}

union all

select day_date, organisation_id, item_id, table_reference, access_level, loaded_timestamp
from {{ ref('int_pln_mvt_depotstoredaysku_visibility') }}
        {% if is_incremental() %}
        where loaded_timestamp > (select max(loaded_timestamp) from {{ this }} where table_reference = 'fact_pln_mvt_depotstoredaysku' )
        {% endif %}
)