{% snapshot zones_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='location_id',
        strategy='check',
        check_cols=['borough', 'zone', 'service_zone']
    )
}}

select
    location_id,
    borough,
    zone,
    service_zone
from {{ source('raw', 'raw_taxi_zones') }}

{% endsnapshot %}