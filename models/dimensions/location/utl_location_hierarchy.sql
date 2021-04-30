WITH RECURSIVE groups AS
(
  -- Anchor Clause
  select  location_id, parent_location_id, to_char(location_id) as Level, location_id location_group_id
  from {{ ref('utl_location_parents') }}
  WHERE parent_location_id is null
  UNION ALL
  -- Recursive Clause
  select  o.location_id, o.parent_location_id, to_char(o.location_id) || '# ' || g.Level, g.location_group_id
  from {{ ref('utl_location_parents') }} o
  join groups g
  on o.parent_location_id = g.location_id
)
SELECT location_id, parent_location_id, Level, location_group_id
FROM groups