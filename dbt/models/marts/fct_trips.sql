{{ config(contract={'enforced': true}) }}

select
    t.trip_id::varchar                                                    as trip_id,
    t._source_month::varchar                                              as source_month,
    t.pickup_at::timestamp                                                as pickup_at,
    t.dropoff_at::timestamp                                               as dropoff_at,
    date_trunc('day', t.pickup_at)::date                                  as pickup_date,
    extract('hour' from t.pickup_at)::integer                             as pickup_hour,
    isodow(t.pickup_at)::integer                                          as pickup_dow,
    t.pickup_zone_id::integer                                             as pickup_zone_id,
    t.dropoff_zone_id::integer                                            as dropoff_zone_id,
    pz.borough::varchar                                                   as pickup_borough,
    pz.zone::varchar                                                      as pickup_zone,
    dz.borough::varchar                                                   as dropoff_borough,
    t.passenger_count::double                                             as passenger_count,
    t.distance_miles::double                                              as distance_miles,
    (date_diff('second', t.pickup_at, t.dropoff_at) / 60.0)::double       as duration_min,
    (t.distance_miles / (date_diff('second', t.pickup_at, t.dropoff_at) / 3600.0))::double as avg_speed_mph,
    t.fare_amount::double                                                 as fare_amount,
    t.tip_amount::double                                                  as tip_amount,
    t.total_amount::double                                                as total_amount,
    case when t.payment_type = 1 and t.fare_amount > 0
         then t.tip_amount / t.fare_amount end::double                    as tip_pct
from {{ ref('stg_trips') }} t
left join {{ ref('taxi_zone_lookup') }} pz on pz.LocationID = t.pickup_zone_id
left join {{ ref('taxi_zone_lookup') }} dz on dz.LocationID = t.dropoff_zone_id
