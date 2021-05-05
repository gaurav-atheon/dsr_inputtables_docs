WITH RECURSIVE groups AS
(
  -- Anchor Clause
  select  organisation_id descendant_organisation_id, parent_organisation_id, to_char(organisation_id) as Level, organisation_id
  from {{ ref('utl_organisation_parents') }}
  UNION ALL
  -- Recursive Clause
  select  o.organisation_id, o.parent_organisation_id, to_char(o.organisation_id) || '->' || g.Level, g.organisation_id
  from {{ ref('utl_organisation_parents') }} o
  join groups g
  on o.parent_organisation_id = g.descendant_organisation_id
)
SELECT organisation_id, descendant_organisation_id, parent_organisation_id, Level
FROM groups