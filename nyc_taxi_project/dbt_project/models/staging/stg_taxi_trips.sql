{{ config(materialized='view') }}

select
    trip_id,
    vendor_id,
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_minutes,

    -- Qualification de la ligne plutot que filtrage : aucune ligne n'est supprimee
    -- ici, pour garder une tracabilite complete depuis le staging (cf. Fiches_Modelisation).
    case
        when fare_amount > 0
            and trip_distance > 0
            and passenger_count > 0
            and tpep_dropoff_datetime > tpep_pickup_datetime
        then true
        else false
    end as is_valid_trip

from {{ source('raw', 'raw_taxi_trips') }}