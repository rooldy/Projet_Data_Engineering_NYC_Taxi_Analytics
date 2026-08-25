{{ config(materialized='table') }}

select
    payment_type_id,
    payment_label
from {{ ref('seed_payment_type') }}
