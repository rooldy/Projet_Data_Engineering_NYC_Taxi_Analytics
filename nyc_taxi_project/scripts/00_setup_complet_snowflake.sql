-- ============================================================
-- Setup complet Snowflake - Projet NYC Taxi Analytics
-- Script unique, integre toutes les corrections rencontrees
-- lors de la premiere mise en place.
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================
-- 1. WAREHOUSE
-- ============================================================
CREATE WAREHOUSE IF NOT EXISTS TAXI_WH
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;

-- ============================================================
-- 2. ROLE DEDIE
-- ============================================================
CREATE ROLE IF NOT EXISTS TAXI_DBT_ROLE;
GRANT ROLE TAXI_DBT_ROLE TO USER ROOLDY002;
GRANT ALL ON WAREHOUSE TAXI_WH TO ROLE TAXI_DBT_ROLE;

-- ============================================================
-- 3. BASES (dev / ci / prod)
-- ============================================================
CREATE DATABASE IF NOT EXISTS NYC_TAXI_ANALYTICS_DEV;
CREATE DATABASE IF NOT EXISTS NYC_TAXI_ANALYTICS_CI;
CREATE DATABASE IF NOT EXISTS NYC_TAXI_ANALYTICS;

GRANT ALL ON DATABASE NYC_TAXI_ANALYTICS_DEV TO ROLE TAXI_DBT_ROLE;
GRANT ALL ON DATABASE NYC_TAXI_ANALYTICS_CI TO ROLE TAXI_DBT_ROLE;
GRANT ALL ON DATABASE NYC_TAXI_ANALYTICS TO ROLE TAXI_DBT_ROLE;

-- ============================================================
-- 4. SCHEMA RAW dans chaque base (dev prioritaire, ci/prod suivent)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS NYC_TAXI_ANALYTICS_DEV.RAW;
CREATE SCHEMA IF NOT EXISTS NYC_TAXI_ANALYTICS_CI.RAW;
CREATE SCHEMA IF NOT EXISTS NYC_TAXI_ANALYTICS.RAW;

-- Droits explicites sur les schemas (etape oubliee la premiere fois -
-- GRANT ALL ON DATABASE ne descend pas automatiquement sur les schemas)
GRANT ALL ON SCHEMA NYC_TAXI_ANALYTICS_DEV.RAW TO ROLE TAXI_DBT_ROLE;
GRANT ALL ON SCHEMA NYC_TAXI_ANALYTICS_CI.RAW TO ROLE TAXI_DBT_ROLE;
GRANT ALL ON SCHEMA NYC_TAXI_ANALYTICS.RAW TO ROLE TAXI_DBT_ROLE;

-- Droits automatiques sur les futures tables de ces schemas
GRANT ALL ON FUTURE TABLES IN SCHEMA NYC_TAXI_ANALYTICS_DEV.RAW TO ROLE TAXI_DBT_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA NYC_TAXI_ANALYTICS_CI.RAW TO ROLE TAXI_DBT_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA NYC_TAXI_ANALYTICS.RAW TO ROLE TAXI_DBT_ROLE;

-- Droits generaux sur les futurs schemas de chaque base (dbt va creer
-- dbt_dev_staging, dbt_dev_analytics, snapshots, etc. automatiquement)
GRANT CREATE SCHEMA ON DATABASE NYC_TAXI_ANALYTICS_DEV TO ROLE TAXI_DBT_ROLE;
GRANT CREATE SCHEMA ON DATABASE NYC_TAXI_ANALYTICS_CI TO ROLE TAXI_DBT_ROLE;
GRANT CREATE SCHEMA ON DATABASE NYC_TAXI_ANALYTICS TO ROLE TAXI_DBT_ROLE;

-- ============================================================
-- 5. TABLES RAW (schema complet, avec trip_id AUTOINCREMENT)
-- ============================================================
USE ROLE TAXI_DBT_ROLE;
USE WAREHOUSE TAXI_WH;

CREATE TABLE IF NOT EXISTS NYC_TAXI_ANALYTICS_DEV.RAW.RAW_TAXI_TRIPS (
    trip_id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    vendor_id INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    rate_code_id INT,
    store_and_fwd_flag STRING,
    pu_location_id INT,
    do_location_id INT,
    payment_type INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    total_amount FLOAT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _source_file STRING
);

CREATE TABLE IF NOT EXISTS NYC_TAXI_ANALYTICS_DEV.RAW.RAW_WEATHER (
    weather_timestamp TIMESTAMP,
    temperature FLOAT,
    precipitation FLOAT,
    weather_condition STRING,
    humidity FLOAT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS NYC_TAXI_ANALYTICS_DEV.RAW.RAW_TAXI_ZONES (
    location_id INT,
    borough STRING,
    zone STRING,
    service_zone STRING
);

-- ============================================================
-- 6. VERIFICATION FINALE
-- ============================================================
SHOW DATABASES LIKE 'NYC_TAXI%';
SHOW WAREHOUSES LIKE 'TAXI_WH';
SHOW GRANTS TO ROLE TAXI_DBT_ROLE;

USE DATABASE NYC_TAXI_ANALYTICS_DEV;
USE SCHEMA RAW;
DESC TABLE RAW_TAXI_TRIPS;
DESC TABLE RAW_WEATHER;
DESC TABLE RAW_TAXI_ZONES;

SELECT 'Setup termine avec succes' AS status;

SELECT COUNT(*) FROM NYC_TAXI_ANALYTICS_DEV.RAW.RAW_TAXI_TRIPS;

ALTER USER ROOLDY002 SET RSA_PUBLIC_KEY='
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA7/mgkVe45xZWj4+PIaBt
LI7ki5aRMZbnTJH8BcLrcEU/4fg7TfNeJlw4k7SamPdTNApXlyTSffP1zRHNOUcJ
vqcemINEH+xBK4iPcJpkEBCjfBi5joWJoglsx0CLUOxQhh/dCKCChRE9XE3i0qDR
wu1/HHl6myUm3ugQaoX7k863dtRCw8aAYms//f/NhenYoSGJZaz30m/gmzvnRjvm
9skS8HIVvqylK2vcjChAwCw7GSt9qwmMGCQJ7xRCvCbaOHRo17mGQS875z8f/LE3
+RiNgU5CsLX6njPaorbLuxMrRFJtFvd96KitQjOHyneGmKHSqjx8/6h2U2/eXLPX
4wIDAQAB
';

DESC USER ROOLDY002;


USE ROLE TAXI_DBT_ROLE;
USE WAREHOUSE TAXI_WH;

CREATE SCHEMA IF NOT EXISTS NYC_TAXI_ANALYTICS_CI.RAW;

CREATE TABLE IF NOT EXISTS NYC_TAXI_ANALYTICS_CI.RAW.RAW_TAXI_TRIPS (
    trip_id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    vendor_id INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    rate_code_id INT,
    store_and_fwd_flag STRING,
    pu_location_id INT,
    do_location_id INT,
    payment_type INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    total_amount FLOAT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _source_file STRING
);

CREATE TABLE IF NOT EXISTS NYC_TAXI_ANALYTICS_CI.RAW.RAW_WEATHER (
    weather_timestamp TIMESTAMP,
    temperature FLOAT,
    precipitation FLOAT,
    weather_condition STRING,
    humidity FLOAT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS NYC_TAXI_ANALYTICS_CI.RAW.RAW_TAXI_ZONES (
    location_id INT,
    borough STRING,
    zone STRING,
    service_zone STRING
);