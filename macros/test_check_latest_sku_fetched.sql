{% macro test_check_latest_sku_fetched(model, column_name,columnlist) %}

{%- set ci =  model ~ "_ci" -%}
{% set test_in_env = kwargs.get('env') %}
{#-
  We should run this test when:
  * the environment has not been specified OR,
  * we are in the specified environment
  -#}
  {%- if test_in_env is none or target.name == test_in_env -%}

        {%- set fields = [] -%}

        with validation_errors as
        (
            (   select
                    {% for field in columnlist -%}
                        {%- set _ = fields.append("coalesce(cast(" ~ field ~ " as " ~ dbt_utils.type_string() ~ "), '')") -%}
                        {%- if not loop.last %} {%- set _ = fields.append("'-'") -%} {%- endif -%}
                    {%- endfor -%}
                    {{dbt_utils.hash(dbt_utils.concat(fields))}} as unique_key,
                    max(to_timestamp(loaded_timestamp))
                from {{ci}}
                group by unique_key
            )
            minus
            (
                select {{column_name}} as unique_key,
                        to_timestamp(loaded_timestamp)
                from {{ model }}
            )
        )

        select count(*)
        from validation_errors
    {%- else -%}

        select 0

    {%- endif -%}
{% endmacro %}