-- Reconciliation: for every month, raw rows = valid rows + quarantined rows. Returns a row only on a mismatch.
with raw as (select _source_month m, count(*) n from {{ source('raw', 'trips') }} group by 1),
     valid as (select _source_month m, count(*) n from {{ ref('stg_trips') }} group by 1),
     bad as (select _source_month m, count(*) n from {{ ref('trips_quarantine') }} group by 1)
select coalesce(raw.m, valid.m, bad.m) as m, coalesce(raw.n, 0) as raw_rows,
       coalesce(valid.n, 0) as valid_rows, coalesce(bad.n, 0) as rejected_rows
from raw
full outer join valid using (m)
full outer join bad using (m)
where coalesce(raw.n, 0) <> coalesce(valid.n, 0) + coalesce(bad.n, 0)
