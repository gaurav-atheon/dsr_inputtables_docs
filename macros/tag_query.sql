{% macro tag_query() %}
{% set tag = 'dbt-' ~ this.database|lower() ~ '-' ~ this.name|lower() %}
alter session set query_tag = '{{ tag }}'
{% endmacro %}