
{% macro grant_usage_to_schemas(schemas, user) %}
  {% for schema in schemas %}
    grant usage on schema {{ schema }} to {{ user }};
  {% endfor %}
{% endmacro %}

