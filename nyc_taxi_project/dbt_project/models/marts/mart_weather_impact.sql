{{ config(materialized='table') }}

with daily_trips as (
    select
        pickup_date,
        count(*) as total_trips,
        round(avg(total_amount), 2) as avg_fare,
        round(sum(total_amount), 2) as total_revenue
    from {{ ref('fct_trips') }}
    where is_valid_trip = true
    group by pickup_date
),

daily_weather as (
    select
        date(weather_timestamp) as weather_date,
        avg(temperature) as avg_temperature,
        sum(precipitation) as total_precipitation,
        max(weather_condition) as dominant_condition
    from {{ ref('stg_weather') }}
    group by date(weather_timestamp)
)

select
    t.pickup_date,
    t.total_trips,
    t.avg_fare,
    t.total_revenue,
    w.avg_temperature,
    w.total_precipitation,
    w.dominant_condition,
    case
        when w.total_precipitation > 5 then 'Rainy Day'
        else 'Normal Day'
    end as day_type
from daily_trips t
left join daily_weather w
    on t.pickup_date = w.weather_date