{{ config(materialized='table') }}

-- Analyse des liaisons origine -> destination (grain = une paire de zones),
-- jusqu'ici jamais exploitee malgre la presence des deux cles dans fct_trips
-- (seule la performance par zone de depart isolee etait analysee).
select
    pu.borough as pickup_borough,
    pu.zone as pickup_zone,
    do.borough as dropoff_borough,
    do.zone as dropoff_zone,
    count(*) as total_trips,
    round(sum(t.total_amount), 2) as total_revenue,
    round(avg(t.total_amount), 2) as avg_fare,
    round(avg(t.trip_distance), 2) as avg_distance,
    round(avg(t.trip_duration_minutes), 2) as avg_trip_duration_minutes
from {{ ref('fct_trips') }} as t
inner join {{ ref('dim_zones') }} as pu
    on t.pickup_location_id = pu.location_id
inner join {{ ref('dim_zones') }} as do
    on t.dropoff_location_id = do.location_id
where t.is_valid_trip = true
group by pu.borough, pu.zone, do.borough, do.zone
order by total_trips desc