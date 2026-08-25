{{ config(materialized='view') }}

select
    weather_timestamp,
    temperature,
    precipitation,
    weather_condition,
    humidity
from {{ source('raw', 'raw_weather') }}
where weather_timestamp is not null