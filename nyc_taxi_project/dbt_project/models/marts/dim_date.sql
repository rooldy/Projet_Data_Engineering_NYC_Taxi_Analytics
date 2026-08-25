{{ config(materialized='table') }}

-- Necessite le package dbt_utils (voir packages.yml + `dbt deps`)
with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2027-01-01' as date)"
    ) }}
)

select
    date_day as date_key,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(dayofweek from date_day) as day_of_week,
    case
        when extract(dayofweek from date_day) in (0, 6) then true
        else false
    end as is_weekend
from date_spine