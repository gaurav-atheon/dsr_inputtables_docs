WITH RECURSIVE groups AS
(

  -- Anchor Clause
  select  organisation_id, parent_organisation_id, to_char(organisation_id) as Level, organisation_id organisation_group_id
  from {{ ref('dim_organisation') }}
  WHERE parent_organisation_id  is null
  UNION ALL
  -- Recursive Clause
  select  o.organisation_id, o.parent_organisation_id, to_char(o.organisation_id) || ', ' || g.Level, g.organisation_group_id
  from {{ ref('dim_organisation') }} o
  join groups g
  on o.parent_organisation_id = g.organisation_id

)
SELECT organisation_id, parent_organisation_id, Level, organisation_group_id
FROM groups