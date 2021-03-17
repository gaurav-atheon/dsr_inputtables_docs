{% macro test_not_null(model) %}

  {% set column_name = kwargs.get('column_name', kwargs.get('arg')) %}

  {% set test_in_env = kwargs.get('env') %}

  {#-
  We should run this test when:
  * the environment has not been specified OR,
  * we are in the specified environment
  -#}
  {%- if test_in_env is none or target.name == test_in_env -%}

    select count(*)
    from {{ model }}
    where {{ column_name }} is null

  {#-
  We should skip this test otherwise, which we do by returning 0
  -#}
  {%- else -%}

    select 0

  {%- endif -%}

{% endmacro %}