{%- macro is_specific_retailer(retailer_name) -%}

{%- set query -%}

select organisation_id
from {{ ref('dim_organisation') }}
where lower(organisation_name) = '{{retailer_name | lower() }}'
and organisation_type = 'retailer'

{%- endset -%}

{%- if execute -%}
    {%- set organisation_id = run_query(query).columns[0][0] -%}
    {%- do return(organisation_id) -%}
{%- endif -%}

{%- endmacro -%}