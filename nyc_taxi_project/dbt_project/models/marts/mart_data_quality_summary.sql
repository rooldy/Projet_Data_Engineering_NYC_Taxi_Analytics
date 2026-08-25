{{ config(materialized='table') }}

-- Monitoring quotidien de la qualite des donnees source : jusqu'ici, le taux
-- de courses invalides (8,1% decouvert manuellement en debut de projet) n'etait
-- visible qu'en requetant a la main. Ce mart l'expose de facon continue, avec
-- le detail par cause, pour detecter une degradation de la source dans le temps.
--
-- Source volontairement stg_taxi_trips (pas fct_trips) : le controle qualite
-- doit se faire au plus pres de la source, avant toute transformation ulterieure.
select
    date(pickup_datetime) as pickup_date,
    count(*) as total_trips,
    sum(case when is_valid_trip then 1 else 0 end) as valid_trips,
    sum(case when not is_valid_trip then 1 else 0 end) as invalid_trips,
    round(
        sum(case when not is_valid_trip then 1 else 0 end) * 100.0 / count(*),
        2
    ) as invalid_rate_pct,

    -- Detail par cause (une course peut cumuler plusieurs causes,
    -- la somme peut donc depasser invalid_trips - cf. lecon tiree en debut de projet
    -- sur IS NULL vs <= 0 en SQL)
    sum(case when fare_amount is null or fare_amount <= 0 then 1 else 0 end) as cause_fare_invalide,
    sum(case when trip_distance is null or trip_distance <= 0 then 1 else 0 end) as cause_distance_invalide,
    sum(case when passenger_count is null or passenger_count <= 0 then 1 else 0 end) as cause_passagers_invalide,
    sum(
        case
            when dropoff_datetime is null or pickup_datetime is null
                or dropoff_datetime <= pickup_datetime
            then 1 else 0
        end
    ) as cause_duree_invalide

from {{ ref('stg_taxi_trips') }}
group by date(pickup_datetime)
order by pickup_date