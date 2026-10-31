select source_month, trip_id, count(*) as n
from {{ ref('fct_trips') }}
group by 1, 2
having count(*) > 1
