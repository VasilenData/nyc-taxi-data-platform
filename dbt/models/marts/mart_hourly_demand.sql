select
    pickup_dow,
    pickup_hour,
    count(*)                                  as trips,
    avg(avg_speed_mph)                        as avg_speed_mph,
    avg(fare_amount)                          as avg_fare
from {{ ref('fct_trips') }}
group by 1, 2
