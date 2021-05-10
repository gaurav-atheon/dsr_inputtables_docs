{% set fact_tables = [
    "fact_act_inv_locationdaycase",
    "fact_act_inv_locationdaysku",
    "fact_pln_inv_storedaysku",
    "fact_act_mvt_depotstoredaysku",
    "fact_act_mvt_storedaysku",
    "fact_pln_mvt_storedaysku",
    "fact_pln_mvt_depotstoredaysku"
    ] %}

{% for fact_table in fact_tables %}
select '{{fact_table}}' as table_reference
    {%- if not loop.last %}
        union
    {% endif %}
{% endfor %}