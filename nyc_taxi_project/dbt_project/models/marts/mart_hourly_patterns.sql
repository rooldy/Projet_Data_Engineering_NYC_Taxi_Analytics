{{ config(materialized='table') }}

-- Analyse des patterns horaires de la demande - exploite enfin dim_date
-- (is_weekend, day_of_week), jamais utilisee jusqu'ici malgre sa construction.
select
    extract(hour from t.pickup_datetime) as pickup_hour,
    d.day_of_week,
    d.is_weekend,
    count(*) as total_trips,
    round(avg(t.total_amount), 2) as avg_fare,
    round(avg(t.trip_duration_minutes), 2) as avg_trip_duration_minutes,
    round(sum(t.total_amount), 2) as total_revenue
from {{ ref('fct_trips') }} as t
inner join {{ ref('dim_date') }} as d
    on t.pickup_date = d.date_key
where t.is_valid_trip = true
group by
    extract(hour from t.pickup_datetime),
    d.day_of_week,
    d.is_weekend
order by pickup_hour, day_of_week