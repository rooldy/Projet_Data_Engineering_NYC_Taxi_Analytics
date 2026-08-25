{{ config(materialized='table') }}

select
    z.borough as pickup_borough,
    z.zone as pickup_zone,
    count(*) as total_trips,
    round(sum(t.total_amount), 2) as total_revenue,
    round(avg(t.total_amount), 2) as avg_fare,
    round(avg(t.trip_duration_minutes), 2) as avg_trip_duration_minutes
from {{ ref('fct_trips') }} as t
inner join {{ ref('dim_zones') }} as z
    on t.pickup_location_id = z.location_id
where t.is_valid_trip = true
group by z.borough, z.zone
order by total_revenue desc