-- Trips that passed every rule. Everything downstream reads this, never raw.
select * exclude (reject_reason)
from {{ ref('stg_trips_classified') }}
where reject_reason is null
