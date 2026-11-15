-- raw_rows repeats for every rejection reason: MAX, never SUM, per month.
select source_month, max(raw_rows) as input_rows,
       sum(rejected_rows) as quarantined_rows,
       max(raw_rows) - sum(rejected_rows) as valid_rows,
       round(100.0 * sum(rejected_rows) / max(raw_rows), 2) as rejected_percent
from main.mart_data_quality
group by source_month
order by source_month;
