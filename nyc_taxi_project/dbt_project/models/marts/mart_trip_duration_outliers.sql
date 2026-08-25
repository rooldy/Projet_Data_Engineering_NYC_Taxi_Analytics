{{ config(materialized='table') }}

-- Detection d'anomalies sur la duree des courses via percentiles (1er et 99e).
-- Grain : une ligne par course identifiee comme anomalie (trop courte ou trop longue),
-- utile pour investiguer des erreurs de capteur, des trajets a l'arret, etc.
with duration_bounds as (
    select
        percentile_cont(0.01) within group (order by trip_duration_minutes) as p1_duration,
        percentile_cont(0.99) within group (order by trip_duration_minutes) as p99_duration
    from {{ ref('fct_trips') }}
    where is_valid_trip = true
)

select
    t.trip_id,
    t.pickup_datetime,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.trip_distance,
    t.trip_duration_minutes,
    t.fare_amount,
    t.total_amount,
    case
        when t.trip_duration_minutes < b.p1_duration then 'Too Short'
        when t.trip_duration_minutes > b.p99_duration then 'Too Long'
    end as outlier_type
from {{ ref('fct_trips') }} as t
cross join duration_bounds as b
where t.is_valid_trip = true
  and (
        t.trip_duration_minutes < b.p1_duration
        or t.trip_duration_minutes > b.p99_duration
      )