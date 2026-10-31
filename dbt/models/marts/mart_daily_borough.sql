select
    pickup_date,
    coalesce(pickup_borough, 'Unknown')       as pickup_borough,
    count(*)                                  as trips,
    sum(total_amount)                         as revenue,
    avg(fare_amount)                          as avg_fare,
    avg(distance_miles)                       as avg_distance_miles,
    avg(duration_min)                         as avg_duration_min,
    avg(tip_pct)                              as avg_tip_pct
from {{ ref('fct_trips') }}
group by 1, 2
