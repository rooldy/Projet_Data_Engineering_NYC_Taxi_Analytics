# Projet Data Engineering - NYC Taxi Analytics
## Plan Complet : Snowflake + dbt + Airflow + Docker

**Objectif** : Construire un pipeline data end-to-end de niveau entreprise, 100% gratuit, démontrant orchestration, transformation, tests et CI/CD.

**Dataset** : NYC Yellow Taxi Trip Data + OpenWeather API (enrichissement météo)

---

## 🎯 Objectifs Pédagogiques du Projet

- Maîtriser dbt en environnement orchestré (pas juste `dbt run` manuel)
- Comprendre l'incrémental réel avec des batches mensuels
- Containeriser un pipeline complet avec Docker
- Mettre en place un vrai CI/CD avec tests automatiques
- Croiser deux sources de données (batch + API)

---

## 🏗️ Architecture Globale

```
┌─────────────────────┐        ┌──────────────────────┐
│  NYC TLC (S3/HTTPS)  │        │  OpenWeather API       │
│  Fichiers Parquet     │        │  Données météo NYC      │
│  mensuels              │        │  (appel horaire/quotidien)│
└──────────┬───────────┘        └───────────┬──────────┘
           │                                 │
           ▼                                 ▼
┌─────────────────────────────────────────────────────┐
│              PYTHON INGESTION SCRIPTS                 │
│         (dans containers Docker séparés)               │
└──────────────────────┬────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────┐
│                   SNOWFLAKE                            │
│   RAW_TAXI  →  STAGING  →  INTERMEDIATE  →  MARTS      │
└──────────────────────┬────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────┐
│                   dbt CORE                             │
│      Modèles, Tests, Snapshots, Documentation           │
└──────────────────────┬────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────┐
│                APACHE AIRFLOW (Docker)                  │
│    Orchestration quotidienne : ingestion -> dbt run       │
└──────────────────────┬────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────┐
│         METABASE (Docker)  +  STREAMLIT                 │
│              Dashboards & Restitution                   │
└─────────────────────────────────────────────────────┘

         GitHub Actions (CI/CD) : dbt run + test sur chaque PR
```

---

## 📦 PARTIE 1 : Sources de Données

### 1.1 NYC Yellow Taxi Trip Data

**Source officielle** : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

**Format** : fichiers Parquet mensuels, hébergés publiquement
```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet
...
```

**Colonnes principales** :

| Colonne | Description |
|---|---|
| `tpep_pickup_datetime` | Date/heure de départ |
| `tpep_dropoff_datetime` | Date/heure d'arrivée |
| `passenger_count` | Nombre de passagers |
| `trip_distance` | Distance en miles |
| `PULocationID` / `DOLocationID` | Zones de départ/arrivée |
| `fare_amount` | Montant de la course |
| `tip_amount` | Pourboire |
| `total_amount` | Montant total |
| `payment_type` | Type de paiement |

**Volume recommandé pour démarrer** : 3 mois de données (~9-12M lignes) — largement suffisant, gérable en trial Snowflake.

### 1.2 OpenWeather API (enrichissement)

**Source** : https://openweathermap.org/api (tier gratuit : 1000 appels/jour)

**Données récupérées** : température, précipitations, conditions météo par heure pour New York.

**But** : croiser la demande de taxi avec la météo (hypothèse : plus de courses quand il pleut).

---

## 🗃️ PARTIE 2 : Architecture Snowflake

### 2.1 Structure des bases et schémas

```sql
-- Création de la base
CREATE DATABASE NYC_TAXI_ANALYTICS;

-- Schémas par couche
CREATE SCHEMA NYC_TAXI_ANALYTICS.RAW;
CREATE SCHEMA NYC_TAXI_ANALYTICS.STAGING;
CREATE SCHEMA NYC_TAXI_ANALYTICS.INTERMEDIATE;
CREATE SCHEMA NYC_TAXI_ANALYTICS.MARTS;

-- Warehouse dédié, optimisé coût
CREATE WAREHOUSE TAXI_WH
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;

-- Rôle dédié au projet
CREATE ROLE TAXI_DBT_ROLE;
GRANT ALL ON DATABASE NYC_TAXI_ANALYTICS TO ROLE TAXI_DBT_ROLE;
```

### 2.2 Tables RAW

```sql
CREATE TABLE RAW.RAW_TAXI_TRIPS (
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    pu_location_id INT,
    do_location_id INT,
    fare_amount FLOAT,
    tip_amount FLOAT,
    total_amount FLOAT,
    payment_type INT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _source_file STRING
);

CREATE TABLE RAW.RAW_WEATHER (
    weather_timestamp TIMESTAMP,
    temperature FLOAT,
    precipitation FLOAT,
    weather_condition STRING,
    humidity FLOAT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE RAW.RAW_TAXI_ZONES (
    location_id INT,
    borough STRING,
    zone STRING,
    service_zone STRING
);
```

---

## 🔧 PARTIE 3 : Structure du Projet dbt

### 3.1 Arborescence du projet

```
nyc_taxi_dbt/
├── dbt_project.yml
├── profiles.yml
├── packages.yml
├── models/
│   ├── staging/
│   │   ├── stg_taxi_trips.sql
│   │   ├── stg_weather.sql
│   │   ├── stg_taxi_zones.sql
│   │   └── _staging.yml
│   ├── intermediate/
│   │   ├── int_trips_with_zones.sql
│   │   ├── int_trips_with_weather.sql
│   │   └── _intermediate.yml
│   └── marts/
│       ├── fct_trips.sql
│       ├── dim_zones.sql
│       ├── dim_date.sql
│       ├── mart_daily_revenue.sql
│       ├── mart_zone_performance.sql
│       ├── mart_weather_impact.sql
│       └── _marts.yml
├── snapshots/
│   └── zones_snapshot.sql
├── tests/
│   └── assert_positive_fares.sql
├── macros/
│   └── generate_schema_name.sql
└── seeds/
    └── taxi_zone_lookup.csv
```

### 3.2 Exemple de modèle Staging

```sql
-- models/staging/stg_taxi_trips.sql
{{ config(materialized='view') }}

SELECT
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    pu_location_id,
    do_location_id,
    fare_amount,
    tip_amount,
    total_amount,
    payment_type,
    DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes
FROM {{ source('raw', 'raw_taxi_trips') }}
WHERE fare_amount > 0
  AND trip_distance > 0
  AND passenger_count > 0
```

### 3.3 Exemple de modèle Incrémental (Marts)

```sql
-- models/marts/fct_trips.sql
{{ 
    config(
        materialized='incremental',
        unique_key='trip_id',
        partition_by={'field': 'pickup_date', 'data_type': 'date'}
    ) 
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['pickup_datetime', 'do_location_id', 'fare_amount']) }} AS trip_id,
    DATE(pickup_datetime) AS pickup_date,
    pickup_datetime,
    dropoff_datetime,
    pu_location_id,
    do_location_id,
    trip_distance,
    trip_duration_minutes,
    fare_amount,
    tip_amount,
    total_amount
FROM {{ ref('int_trips_with_zones') }}

{% if is_incremental() %}
WHERE DATE(pickup_datetime) > (SELECT MAX(pickup_date) FROM {{ this }})
{% endif %}
```

### 3.4 Exemple de modèle Mart Business

```sql
-- models/marts/mart_weather_impact.sql
{{ config(materialized='table') }}

WITH daily_trips AS (
    SELECT
        pickup_date,
        COUNT(*) AS total_trips,
        AVG(total_amount) AS avg_fare,
        SUM(total_amount) AS total_revenue
    FROM {{ ref('fct_trips') }}
    GROUP BY pickup_date
),

daily_weather AS (
    SELECT
        DATE(weather_timestamp) AS weather_date,
        AVG(temperature) AS avg_temperature,
        SUM(precipitation) AS total_precipitation,
        MODE(weather_condition) AS dominant_condition
    FROM {{ ref('stg_weather') }}
    GROUP BY DATE(weather_timestamp)
)

SELECT
    t.pickup_date,
    t.total_trips,
    t.avg_fare,
    t.total_revenue,
    w.avg_temperature,
    w.total_precipitation,
    w.dominant_condition,
    CASE 
        WHEN w.total_precipitation > 5 THEN 'Rainy Day'
        ELSE 'Normal Day'
    END AS day_type
FROM daily_trips t
LEFT JOIN daily_weather w ON t.pickup_date = w.weather_date
```

### 3.5 Fichier de tests YAML

```yaml
# models/marts/_marts.yml
version: 2

models:
  - name: fct_trips
    description: "Table de faits des courses de taxi, une ligne par course"
    columns:
      - name: trip_id
        description: "Cle unique generee pour chaque course"
        tests:
          - unique
          - not_null
      - name: fare_amount
        tests:
          - not_null
      - name: pu_location_id
        tests:
          - relationships:
              to: ref('dim_zones')
              field: location_id

  - name: mart_daily_revenue
    description: "Agregation quotidienne du revenu et du volume de courses"
    columns:
      - name: pickup_date
        tests:
          - unique
          - not_null
```

### 3.5bis Environnements dev / ci / prod — séparation réelle

**Pourquoi c'est critique** : c'est la première question qu'un tech lead pose. Sans ça, chaque `dbt run` local écrase directement la production.

```yaml
# profiles.yml
nyc_taxi_dbt:
  target: dev
  outputs:

    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: TAXI_DBT_ROLE
      database: NYC_TAXI_ANALYTICS_DEV
      warehouse: TAXI_WH
      schema: dbt_dev
      threads: 4

    ci:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: TAXI_DBT_ROLE
      database: NYC_TAXI_ANALYTICS_CI
      warehouse: TAXI_WH
      schema: dbt_ci
      threads: 4

    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: TAXI_DBT_ROLE
      database: NYC_TAXI_ANALYTICS
      warehouse: TAXI_WH
      schema: analytics
      threads: 8
```

**Workflow associé** :
- `dbt run` en local → cible `dev` par défaut, chacun travaille dans sa propre base sans casser les autres
- Pull request → GitHub Actions build/test dans `ci` (base jetable, recréée à chaque run)
- Merge sur `main` → déploiement dans `prod` uniquement via le DAG Airflow, jamais en local

```sql
-- Créer les 3 bases correspondantes dans Snowflake
CREATE DATABASE NYC_TAXI_ANALYTICS_DEV;
CREATE DATABASE NYC_TAXI_ANALYTICS_CI;
-- NYC_TAXI_ANALYTICS (prod) déjà créée en Partie 2
```

### 3.5ter packages.yml complet

```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
  - package: calogica/dbt_expectations
    version: 0.10.1
  - package: elementary-data/elementary
    version: 0.15.2
```

- **dbt_utils** : fonctions utilitaires (surrogate keys, date_spine, etc.)
- **dbt_expectations** : tests statistiques avancés (distribution, plages de valeurs, cohérence de types) inspirés de Great Expectations
- **elementary** : observabilité des données (voir Partie 8)

**Exemple de test dbt_expectations** :
```yaml
# models/marts/_marts.yml (ajout)
      - name: total_amount
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 2000
          - dbt_expectations.expect_column_mean_to_be_between:
              min_value: 5
              max_value: 100
```

### 3.5quater dbt Exposures — lineage jusqu'à la restitution

Documente que vos marts alimentent réellement des dashboards, complète le DAG dbt jusqu'à la couche BI.

```yaml
# models/marts/_exposures.yml
version: 2

exposures:
  - name: metabase_revenue_dashboard
    label: "Dashboard Revenu Quotidien"
    type: dashboard
    maturity: high
    url: https://votre-metabase-url/dashboard/1
    depends_on:
      - ref('mart_daily_revenue')
      - ref('mart_zone_performance')
    owner:
      name: Rooldy Alphonse
      email: votre-email@example.com

  - name: streamlit_weather_impact
    label: "App Streamlit - Impact Meteo"
    type: application
    maturity: medium
    depends_on:
      - ref('mart_weather_impact')
    owner:
      name: Rooldy Alphonse
      email: votre-email@example.com
```

### 3.6 Snapshot (pour démontrer le SCD Type 2)

```sql
-- snapshots/zones_snapshot.sql
{% snapshot zones_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='location_id',
        strategy='check',
        check_cols=['borough', 'zone', 'service_zone']
    )
}}

SELECT * FROM {{ source('raw', 'raw_taxi_zones') }}

{% endsnapshot %}
```

---

## 🌬️ PARTIE 4 : Orchestration Airflow

### 4.1 Structure des fichiers

```
nyc_taxi_project/
├── docker-compose.yml
├── docker/
│   ├── Dockerfile.airflow
│   └── Dockerfile.ingestion
├── dags/
│   ├── ingestion_dag.py
│   └── dbt_pipeline_dag.py
├── scripts/
│   ├── ingest_taxi_data.py
│   └── ingest_weather_data.py
├── dbt_project/
│   └── (structure vue plus haut)
├── requirements.txt
└── .env
```

### 4.2 Script d'ingestion Python (taxi) — version standard entreprise

**Ce qui change par rapport à une version naive** :
- **Idempotence** : suppression du mois avant réinsertion (pattern delete+insert), pour pouvoir relancer le DAG sans dupliquer les données
- **Gestion d'erreurs** : try/except avec logging structuré, pas de plantage silencieux
- **Logging** : traçabilité de chaque étape (utile pour debug en prod)

```python
# scripts/ingest_taxi_data.py
import snowflake.connector
import pandas as pd
import requests
import logging
import sys
from datetime import datetime
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IngestionError(Exception):
    """Erreur levée en cas d'échec critique de l'ingestion"""
    pass


def download_taxi_data(year: int, month: int) -> str:
    """Telecharge un fichier Parquet mensuel NYC Taxi"""
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    local_path = f"/tmp/taxi_{year}_{month:02d}.parquet"

    try:
        logger.info(f"Telechargement depuis {url}")
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Fichier telecharge : {local_path}")
        return local_path

    except requests.exceptions.RequestException as e:
        logger.error(f"Echec du telechargement : {e}")
        raise IngestionError(f"Impossible de telecharger {url}") from e


def get_snowflake_connection():
    """Cree une connexion Snowflake avec gestion d'erreur explicite"""
    try:
        return snowflake.connector.connect(
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            user=os.environ['SNOWFLAKE_USER'],
            password=os.environ['SNOWFLAKE_PASSWORD'],
            warehouse='TAXI_WH',
            database='NYC_TAXI_ANALYTICS',
            schema='RAW'
        )
    except KeyError as e:
        raise IngestionError(f"Variable d'environnement manquante : {e}")
    except snowflake.connector.errors.Error as e:
        raise IngestionError(f"Connexion Snowflake echouee : {e}")


def load_to_snowflake(file_path: str, source_file: str):
    """
    Charge le fichier Parquet dans Snowflake RAW.
    Idempotent : supprime les donnees du meme fichier source avant reinsertion,
    ce qui permet de relancer le script sans dupliquer les lignes.
    """
    conn = None
    try:
        df = pd.read_parquet(file_path)

        if df.empty:
            raise IngestionError(f"Fichier vide : {file_path}")

        df['_source_file'] = source_file
        df['_loaded_at'] = datetime.now()

        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # --- IDEMPOTENCE : on supprime avant de reinserer ---
        logger.info(f"Suppression des donnees existantes pour {source_file}")
        cursor.execute(
            "DELETE FROM RAW_TAXI_TRIPS WHERE _source_file = %s",
            (source_file,)
        )
        deleted_rows = cursor.rowcount
        logger.info(f"{deleted_rows} lignes supprimees (si existantes)")

        # --- Insertion ---
        from snowflake.connector.pandas_tools import write_pandas
        success, nchunks, nrows, _ = write_pandas(
            conn, df, 'RAW_TAXI_TRIPS'
        )

        if not success:
            raise IngestionError(f"Echec du chargement pour {source_file}")

        conn.commit()
        logger.info(f"{nrows} lignes chargees avec succes depuis {source_file}")

    except IngestionError:
        raise
    except Exception as e:
        logger.error(f"Erreur inattendue lors du chargement : {e}")
        if conn:
            conn.rollback()
        raise IngestionError(f"Echec du chargement de {source_file}") from e
    finally:
        if conn:
            conn.close()


def main(year: int, month: int):
    try:
        file_path = download_taxi_data(year, month)
        load_to_snowflake(file_path, f"yellow_tripdata_{year}-{month:02d}.parquet")
        logger.info("Ingestion terminee avec succes")
    except IngestionError as e:
        logger.error(f"INGESTION ECHOUEE : {e}")
        sys.exit(1)  # Code de sortie non-zero => Airflow marque la tache en echec


if __name__ == "__main__":
    year, month = int(sys.argv[1]), int(sys.argv[2])
    main(year, month)
```

### 4.2bis Tests unitaires (pytest)

Un vrai projet d'entreprise teste au moins les fonctions critiques, indépendamment de la base de données.

```python
# tests/test_ingest_taxi_data.py
import pytest
from unittest.mock import patch, MagicMock
from scripts.ingest_taxi_data import download_taxi_data, IngestionError


def test_download_taxi_data_success():
    with patch('scripts.ingest_taxi_data.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b'fake_data']
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        path = download_taxi_data(2024, 1)
        assert path == "/tmp/taxi_2024_01.parquet"


def test_download_taxi_data_failure_raises_ingestion_error():
    with patch('scripts.ingest_taxi_data.requests.get') as mock_get:
        mock_get.side_effect = Exception("Network error")

        with pytest.raises(IngestionError):
            download_taxi_data(2024, 1)
```

```bash
# Lancer les tests
pytest tests/ -v
```

### 4.3 Script d'ingestion Python (météo)

```python
# scripts/ingest_weather_data.py
import requests
import snowflake.connector
import os
from datetime import datetime

def fetch_weather_data():
    """Recupere la meteo actuelle de New York"""
    api_key = os.getenv('OPENWEATHER_API_KEY')
    url = f"https://api.openweathermap.org/data/2.5/weather?q=New York&appid={api_key}&units=metric"
    
    response = requests.get(url)
    data = response.json()
    
    return {
        'weather_timestamp': datetime.now(),
        'temperature': data['main']['temp'],
        'precipitation': data.get('rain', {}).get('1h', 0),
        'weather_condition': data['weather'][0]['main'],
        'humidity': data['main']['humidity']
    }

def load_weather_to_snowflake(weather_data: dict):
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse='TAXI_WH',
        database='NYC_TAXI_ANALYTICS',
        schema='RAW'
    )
    
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO RAW_WEATHER 
        (weather_timestamp, temperature, precipitation, weather_condition, humidity)
        VALUES (%(weather_timestamp)s, %(temperature)s, %(precipitation)s, 
                %(weather_condition)s, %(humidity)s)
    """, weather_data)
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    weather = fetch_weather_data()
    load_weather_to_snowflake(weather)
    print(f"Meteo chargee : {weather}")
```

### 4.4 DAG Airflow - Pipeline Complet (avec alerting Slack)

**Ajout standard entreprise** : une callback `on_failure` qui poste automatiquement sur Slack si une tâche échoue. Gratuit via un webhook Slack (Incoming Webhooks, aucun coût).

```python
# dags/dbt_pipeline_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow/scripts')


def slack_failure_alert(context):
    """Callback déclenchée automatiquement à l'échec de n'importe quelle tâche"""
    task_instance = context.get('task_instance')
    message = (
        f":red_circle: *Echec Pipeline NYC Taxi*\n"
        f"*Tache* : {task_instance.task_id}\n"
        f"*DAG* : {task_instance.dag_id}\n"
        f"*Date d'execution* : {context.get('execution_date')}\n"
        f"*Log* : {task_instance.log_url}"
    )
    slack_alert = SlackWebhookOperator(
        task_id='slack_failure_notification',
        slack_webhook_conn_id='slack_webhook',
        message=message,
    )
    return slack_alert.execute(context=context)


default_args = {
    'owner': 'rooldy',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'on_failure_callback': slack_failure_alert,
}

with DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    description='Pipeline complet NYC Taxi : ingestion + transformation + tests',
    schedule_interval='@daily',
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['dbt', 'taxi', 'snowflake'],
) as dag:

    # === INGESTION ===
    ingest_weather = PythonOperator(
        task_id='ingest_weather_data',
        python_callable=lambda: __import__('ingest_weather_data').main(),
    )

    # === TRANSFORMATION DBT (target=prod : seul le DAG deploie en prod) ===
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/dbt_project && dbt deps',
    )

    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='cd /opt/airflow/dbt_project && dbt run --select staging --target prod',
    )

    dbt_run_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command='cd /opt/airflow/dbt_project && dbt run --select intermediate --target prod',
    )

    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command='cd /opt/airflow/dbt_project && dbt run --select marts --target prod',
    )

    # === TESTS ===
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_project && dbt test --target prod',
    )

    # === SNAPSHOTS ===
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command='cd /opt/airflow/dbt_project && dbt snapshot --target prod',
    )

    # === DOCUMENTATION ===
    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command='cd /opt/airflow/dbt_project && dbt docs generate --target prod',
    )

    # === DEPENDANCES ===
    ingest_weather >> dbt_deps >> dbt_run_staging >> dbt_run_intermediate >> dbt_run_marts
    dbt_run_marts >> dbt_test >> dbt_snapshot >> dbt_docs
```

**Configuration du webhook Slack (gratuit)** :
1. Créer une app Slack sur https://api.slack.com/apps
2. Activer "Incoming Webhooks"
3. Copier l'URL du webhook dans une Airflow Connection (`slack_webhook`)

```bash
# Ajouter la connexion via CLI Airflow (ou via l'UI)
airflow connections add 'slack_webhook' \
    --conn-type 'slackwebhook' \
    --conn-password 'https://hooks.slack.com/services/VOTRE/WEBHOOK/URL'
```

### 4.5 docker-compose.yml complet

```yaml
version: '3.8'

x-airflow-common: &airflow-common
  build:
    context: .
    dockerfile: docker/Dockerfile.airflow
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    SNOWFLAKE_ACCOUNT: ${SNOWFLAKE_ACCOUNT}
    SNOWFLAKE_USER: ${SNOWFLAKE_USER}
    SNOWFLAKE_PASSWORD: ${SNOWFLAKE_PASSWORD}
    OPENWEATHER_API_KEY: ${OPENWEATHER_API_KEY}
  volumes:
    - ./dags:/opt/airflow/dags
    - ./dbt_project:/opt/airflow/dbt_project
    - ./scripts:/opt/airflow/scripts
    - ./logs:/opt/airflow/logs
  depends_on:
    - postgres

services:
  postgres:
    image: postgres:14
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres_data:/var/lib/postgresql/data

  airflow-init:
    <<: *airflow-common
    command: version
    entrypoint: /bin/bash -c "airflow db init && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com"

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler

  metabase:
    image: metabase/metabase
    ports:
      - "3000:3000"

volumes:
  postgres_data:
```

### 4.6 Dockerfile.airflow

```dockerfile
FROM apache/airflow:2.8.0-python3.9

USER root
RUN apt-get update && apt-get install -y git curl

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```

### 4.7 requirements.txt

```
dbt-snowflake==1.7.0
snowflake-connector-python==3.6.0
pandas==2.1.4
pyarrow==14.0.1
requests==2.31.0
apache-airflow-providers-snowflake==5.2.0
```

---

## ⚙️ PARTIE 5 : CI/CD GitHub Actions

```yaml
# .github/workflows/dbt_ci.yml
name: dbt CI

on:
  pull_request:
    branches: [main]
    paths:
      - 'dbt_project/**'

jobs:
  dbt_test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      
      - name: Install dbt
        run: pip install dbt-snowflake
      
      - name: Configure dbt profile
        run: |
          mkdir -p ~/.dbt
          echo "${{ secrets.DBT_PROFILES_YML }}" > ~/.dbt/profiles.yml
      
      - name: dbt deps
        run: cd dbt_project && dbt deps
      
      - name: dbt run (CI target)
        run: cd dbt_project && dbt run --target ci
      
      - name: dbt test
        run: cd dbt_project && dbt test --target ci
      
      - name: dbt docs generate
        run: cd dbt_project && dbt docs generate --target ci
```

---

## 📊 PARTIE 6 : Modèles Marts Business à construire

| Mart | Objectif | Complexité |
|---|---|---|
| `mart_daily_revenue` | Revenu quotidien, nombre de courses | Facile |
| `mart_zone_performance` | Performance par zone de pickup/dropoff | Moyenne |
| `mart_weather_impact` | Correlation meteo/demande | Moyenne |
| `mart_hourly_patterns` | Patterns horaires (heures de pointe) | Moyenne |
| `mart_payment_analysis` | Analyse par type de paiement, tips | Facile |
| `mart_trip_duration_outliers` | Detection d'anomalies (courses trop longues/courtes) | Avancee |

---

## 📅 PARTIE 7 : Planning d'Exécution

### Semaine 1 : Fondations
- [ ] Setup Snowflake (base, schémas, warehouse, rôle)
- [ ] Setup Docker Compose (Postgres + Airflow de base)
- [ ] Test connexion Airflow ↔ Snowflake
- [ ] Télécharger et charger manuellement 1 mois de données taxi

### Semaine 2 : dbt Core
- [ ] Initialiser projet dbt (`dbt init`)
- [ ] Construire les modèles staging
- [ ] Construire les modèles intermediate
- [ ] Construire les premiers marts (`fct_trips`, `dim_zones`)
- [ ] Ajouter les tests YAML de base

### Semaine 3 : Orchestration Airflow
- [ ] Écrire les scripts d'ingestion Python (taxi + météo)
- [ ] Construire le DAG complet
- [ ] Tester l'exécution end-to-end dans Airflow
- [ ] Ajouter les snapshots dbt

### Semaine 4 : CI/CD & Finalisation
- [ ] Configurer GitHub Actions
- [ ] Ajouter Elementary pour l'observabilité
- [ ] Construire les dashboards Metabase
- [ ] Documentation complète (README, architecture)
- [ ] Préparer le pitch entretien

---

## 🎤 Argument Final pour Entretien

> "J'ai construit un pipeline data engineering complet sur les données de taxis new-yorkais, croisées avec des données météo en temps réel. L'ensemble tourne dans Docker Compose : Airflow orchestre quotidiennement l'ingestion (fichiers mensuels TLC + API météo), suivi d'un pipeline dbt structuré en staging/intermediate/marts avec tests automatisés et snapshots pour l'historisation. Chaque pull request déclenche un run et des tests dbt via GitHub Actions avant merge. Le tout alimente des dashboards Metabase pour l'analyse business — par exemple, l'impact de la météo sur la demande de courses."

---

## 🔗 Ressources Utiles

- NYC TLC Data : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- OpenWeather API : https://openweathermap.org/api
- dbt-snowflake adapter : https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup
- Airflow + dbt best practices : https://docs.getdbt.com/blog/dbt-airflow-spiritual-alignment
- Elementary (data observability) : https://docs.elementary-data.com/

---

**Prochaine étape recommandée** : commencer par la Semaine 1 (setup Snowflake + Docker) avant d'écrire le premier modèle dbt.
