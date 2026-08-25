{{ config(materialized='table') }}

-- S'appuie sur le snapshot SCD2 plutot que sur stg_taxi_zones directement,
-- pour exposer uniquement la version courante de chaque zone tout en conservant
-- l'historique complet des changements dans le snapshot lui-meme.
--
-- COALESCE sur borough/zone/service_zone : certaines zones du referentiel NYC TLC
-- (ex: location_id 264/265, zones "Unknown"/"N/A") ont des champs textuels NULL
-- dans la source brute. On normalise vers 'Unknown' pour garantir qu'aucun
-- consommateur en aval (marts, BI) ne recoive de NULL sur ces colonnes.
select
    location_id,
    coalesce(borough, 'Unknown') as borough,
    coalesce(zone, 'Unknown') as zone,
    coalesce(service_zone, 'Unknown') as service_zone
from {{ ref('zones_snapshot') }}
where dbt_valid_to is null