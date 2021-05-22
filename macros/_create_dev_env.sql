  
{% macro clone_prod_to_target(from) %}

    {% set grant_ownership_sql %}
        CALL grant_schema_permissions('{{target.schema}}');
    {% endset %}
    {% set sql -%}
        create or replace schema {{ target.database }}.{{ target.schema }} clone {{ from }};
    {%- endset %}

    {{ dbt_utils.log_info("Cloning schema " ~ from ~ " into target schema " ~ target.database.lower() ~ "." ~ target.schema.lower() ~ ".") }}

    {% do run_query(sql) %}

    {{ dbt_utils.log_info("Cloned schema " ~ from ~ " into target schema " ~ target.database.lower() ~ "." ~ target.schema.lower() ~ ".") }}

    {{ dbt_utils.log_info("Changing objects ownership.") }}

    {% do run_query(grant_ownership_sql) %}

    {{ dbt_utils.log_info("Objects ownership changed.\nDone.") }}

{% endmacro %}