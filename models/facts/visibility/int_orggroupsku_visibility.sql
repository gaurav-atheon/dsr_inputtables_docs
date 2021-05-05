  SELECT v.table_reference,
         v.item_id,
         d.DAY_DATE,
         o.organisation_group_id,
         v.location_function
         --min(access_level) access_level --This will break the SCD partitioning, so leave out of results
  FROM   {{ ref('utl_organisationsku_visibility') }} v
         left outer join {{ ref('utl_organisation_hierarchy') }} o
                      ON v.SUBJECT_ORGANISATION_ID = o.organisation_id
         inner join {{ ref('dim_date') }} d
                 ON d.DAY_DATE BETWEEN v.day_date_from AND v.day_date_to
  GROUP  BY v.table_reference,
            v.item_id,
            d.DAY_DATE,
            o.organisation_group_id,
            v.location_function