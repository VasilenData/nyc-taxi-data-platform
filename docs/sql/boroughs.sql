-- Historical observed sample totals, not representative NYC demand.
-- revenue in the existing mart is sum(total_amount), not net earnings.
select pickup_borough, sum(trips) as observed_valid_trips,
       round(sum(revenue), 2) as recorded_total_amount_usd,
       min(pickup_date) as first_observed_date, max(pickup_date) as last_observed_date
from main.mart_daily_borough
group by pickup_borough
order by observed_valid_trips desc, pickup_borough;
