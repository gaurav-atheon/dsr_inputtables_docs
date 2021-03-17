{% macro test_check_distinct_sku_fetched(model, column_name) %}
{% set test_in_env = kwargs.get('env') %}
  {#-
  We should run this test when:
  * the environment has not been specified OR,
  * we are in the specified environment
  -#}
  {%- if test_in_env is none or target.name == test_in_env -%}
        with validation_errors as (

            select
                {{ column_name }} as unique_id,
                count(1) count_load_data
            from {{ model }}
             group by 1
             having count_load_data>1
        )

        select count(*)
        from validation_errors
    {%- else -%}

        select 0

    {%- endif -%}
{% endmacro %}