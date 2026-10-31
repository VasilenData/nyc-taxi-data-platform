-- One row per month and rule: how many rows each validation rule rejected. Feeds the quality gate and the report.
with totals as (
    select _source_month as source_month, count(*) as raw_rows
    from {{ source('raw', 'trips') }}
    group by 1
),
rejected as (
    select _source_month as source_month, reject_reason, count(*) as rejected_rows
    from {{ ref('trips_quarantine') }}
    group by 1, 2
)
select
    t.source_month,
    t.raw_rows,
    coalesce(r.reject_reason, 'none')                         as reject_reason,
    coalesce(r.rejected_rows, 0)                              as rejected_rows,
    coalesce(r.rejected_rows, 0) * 1.0 / t.raw_rows           as share_of_month
from totals t
left join rejected r using (source_month)
