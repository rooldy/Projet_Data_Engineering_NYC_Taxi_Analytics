{{
    config(
        materialized='incremental',
        unique_key='trip_id'
    )
}}

select
    stg.trip_id,
    date(stg.pickup_datetime) as pickup_date,
    stg.pickup_datetime,
    stg.dropoff_datetime,

    -- Cles etrangeres, avec COALESCE vers le code 99 "Unknown" pour eviter
    -- tout NULL sur une FK et garantir que les tests relationships ne
    -- puissent jamais echouer silencieusement sur des valeurs absentes.
    stg.pu_location_id as pickup_location_id,
    stg.do_location_id as dropoff_location_id,
    coalesce(stg.vendor_id, 99) as vendor_id,
    coalesce(stg.rate_code_id, 99) as rate_code_id,
    coalesce(stg.payment_type, 99) as payment_type_id,

    -- Mesures
    stg.passenger_count,
    stg.trip_distance,
    stg.trip_duration_minutes,
    stg.fare_amount,
    stg.extra,
    stg.mta_tax,
    stg.tip_amount,
    stg.tolls_amount,
    stg.improvement_surcharge,
    stg.congestion_surcharge,
    stg.airport_fee,
    stg.total_amount,

    -- Attributs techniques
    stg.store_and_fwd_flag,
    stg.is_valid_trip

from {{ ref('stg_taxi_trips') }} as stg

{% if is_incremental() %}
where date(stg.pickup_datetime) > (select max(pickup_date) from {{ this }})
{% endif %}