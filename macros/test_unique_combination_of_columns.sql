{% macro test_unique_combination_of_columns(model, quote_columns = false) %}

  {%- set columns = kwargs.get('combination_of_columns', kwargs.get('arg')) %}
  {% set test_in_env = kwargs.get('env') %}

  {#-
  We should run this test when:
  * the environment has not been specified OR,
  * we are in the specified environment
  -#}
  {%- if test_in_env is none or target.name == test_in_env -%}
    {% if not quote_columns %}
        {%- set column_list=columns %}
    {% elif quote_columns %}
        {%- set column_list=[] %}
            {% for column in columns -%}
                {% set column_list = column_list.append( adapter.quote(column) ) %}
            {%- endfor %}
    {% else %}
        {{ exceptions.raise_compiler_error(
            "`quote_columns` argument for unique_combination_of_columns test must be one of [True, False] Got: '" ~ quote ~"'.'"
        ) }}
    {% endif %}

    {%- set columns_csv=column_list | join(', ') %}


    with validation_errors as (

        select
            {{ columns_csv }}
        from {{ model }}

        group by {{ columns_csv }}
        having count(1) > 1

    )

    select count(*)
    from validation_errors

  {#-
  We should skip this test otherwise, which we do by returning 0
  -#}
  {%- else -%}

    select 0

  {%- endif -%}
{% endmacro %}
