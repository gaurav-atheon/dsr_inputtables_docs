select
        day_date,
        organisation_id,
        LOCATION_ID,
        Product_ID,
        sum(stock_units) as stock_units
        from (

            select
                day_date,
                organisation_id, --converted to DSR ID
                LOCATION_ID, --converted to DSR ID
                Product_ID, --converted to DSR ID
                stock_units,
                loaded_timestamp

            from {{ ref('fact_act_inv_locationdaysku') }}

            union all

            select
                day_date,
                organisation_id, --converted to DSR ID
                LOCATION_ID, --converted to DSR ID
                Product_ID, --converted to DSR ID
                (stock_units*case_size) as stock_units,
                loaded_timestamp

            from {{ ref('fact_act_inv_locationdaycase') }} )

group by day_date,organisation_id,LOCATION_ID,Product_ID