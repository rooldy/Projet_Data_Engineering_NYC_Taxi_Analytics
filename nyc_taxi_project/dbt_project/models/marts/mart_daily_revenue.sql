{{ config(materialized='table') }}

select
    pickup_date,
    count(*) as total_trips,
    round(avg(total_amount), 2) as avg_fare,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(trip_distance), 2) as avg_distance,
    round(avg(tip_amount), 2) as avg_tip
from {{ ref('fct_trips') }}
where is_valid_trip = true
group by pickup_date
order by pickup_date-- test CI/CD
