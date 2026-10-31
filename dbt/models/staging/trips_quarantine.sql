-- Trips that broke a rule, with the reason. Nothing is silently dropped.
select *
from {{ ref('stg_trips_classified') }}
where reject_reason is not null
