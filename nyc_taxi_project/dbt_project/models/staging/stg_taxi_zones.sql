{{ config(materialized='view') }}

select
    location_id,
    borough,
    zone,
    service_zone
from {{ source('raw', 'raw_taxi_zones') }}