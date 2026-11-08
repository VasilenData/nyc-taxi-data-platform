{{ config(
    materialized='incremental',
    unique_key='_source_month',
    incremental_strategy='delete+insert'
) }}

-- One row per raw trip, each tagged with the FIRST rule it breaks (null = valid).
-- Incremental by month: a month that is (re)loaded in raw is fully reprocessed, others are untouched.
-- Rejected rows are kept, never dropped, so raw = valid + rejected can be proved in a test.

with raw as (

    select
        *,
        md5(concat_ws('|', "VendorID", tpep_pickup_datetime, tpep_dropoff_datetime,
                      "PULocationID", "DOLocationID", trip_distance, fare_amount, total_amount)) as trip_id
    from {{ source('raw', 'trips') }} source_rows
    {% if is_incremental() %}
    -- Compare each month's load version to its own processed version. A global
    -- high-water mark skips corrected months when timestamps arrive out of order
    -- (for example after restoring a warehouse or a host clock correction).
    where not exists (
        select 1 from {{ this }} processed
        where processed._source_month = source_rows._source_month
          and processed._loaded_at = source_rows._loaded_at
    )
    {% endif %}

),

ranked as (

    select
        *,
        row_number() over (partition by _source_month, trip_id order by _loaded_at) as occurrence
    from raw

),

classified as (

    select
        trip_id,
        _source_month,
        _source_file,
        _loaded_at,
        "VendorID"               as vendor_id,
        tpep_pickup_datetime     as pickup_at,
        tpep_dropoff_datetime    as dropoff_at,
        passenger_count,
        trip_distance            as distance_miles,
        "PULocationID"           as pickup_zone_id,
        "DOLocationID"           as dropoff_zone_id,
        payment_type,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        case
            when tpep_pickup_datetime is null or tpep_dropoff_datetime is null
                then 'missing_timestamp'
            when tpep_dropoff_datetime <= tpep_pickup_datetime
                then 'non_positive_duration'
            when date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) >= {{ var('max_trip_hours') }} * 3600
                then 'implausible_duration'
            when strftime(tpep_pickup_datetime, '%Y-%m') <> _source_month
                then 'out_of_period'
            when trip_distance is null or trip_distance <= 0 or trip_distance > {{ var('max_trip_miles') }}
                then 'bad_distance'
            when fare_amount is null or fare_amount < 0 or total_amount is null or total_amount <= 0
                then 'bad_fare'
            when "PULocationID" is null or "PULocationID" not between 1 and 265
              or "DOLocationID" is null or "DOLocationID" not between 1 and 265
                then 'unknown_zone'
            when occurrence > 1
                then 'duplicate'
        end as reject_reason
    from ranked

)

select * from classified
