-- The fact table must hold exactly the valid trips: the zone joins may not drop or fan out rows.
select (select count(*) from {{ ref('stg_trips') }}) as valid_rows,
       (select count(*) from {{ ref('fct_trips') }}) as fact_rows
where (select count(*) from {{ ref('stg_trips') }}) <> (select count(*) from {{ ref('fct_trips') }})
