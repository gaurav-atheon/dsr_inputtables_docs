{%- macro scd_from_densified_dates(table_name, date_column) -%}

{%- set partition_columns = dbt_utils.star(ref(table_name), FALSE, date_column) -%}

with
events as
(
select *
from {{ ref(table_name) }}
),

next_event as
(
select e.*,
  lead({{date_column}}) over (partition by {{partition_columns}} order by {{date_column}} asc) next_event_date,
  datediff(day, {{date_column}}, lead({{date_column}}) over (partition by {{partition_columns}} order by {{date_column}} desc)) diff_days_desc,
  datediff(day, {{date_column}}, lead({{date_column}}) over (partition by {{partition_columns}} order by {{date_column}} asc)) diff_days_asc,
  case
    when datediff(day, {{date_column}}, lead({{date_column}}) over (partition by {{partition_columns}} order by {{date_column}} desc)) < -1
        then 'Start'
    when datediff(day, {{date_column}}, lead({{date_column}}) over (partition by {{partition_columns}} order by {{date_column}} desc)) is null
        then 'Start'
    else 'Continuous'
  end marker_start,
  case
    when datediff(day, {{date_column}}, lead({{date_column}}) over (partition by {{partition_columns}} order by {{date_column}} asc)) > 1
        then 'End'
    when datediff(day, {{date_column}}, lead({{date_column}}) over (partition by {{partition_columns}} order by {{date_column}} asc)) is null
        then 'End'
    else 'Continuous'
  end marker_end
  from events e
),

end_points as
(
select evt.*, lead({{date_column}}) over (partition by {{partition_columns}} order by {{date_column}} asc) end_of_run
from next_event evt
--Knock out the intervening dates
where MARKER_START <> 'Continuous'
or MARKER_END <> 'Continuous'
),

scd as (
select {{date_column}} DAY_DATE_FROM,
  case
  when MARKER_END = 'End' then {{date_column}}
  when END_OF_RUN is null then {{date_column}}
  else END_OF_RUN end DAY_DATE_TO,
{{partition_columns}}
from end_points q1
where MARKER_START = 'Start' --Only keep the start of a run
)

select *
from scd

{%- endmacro -%}