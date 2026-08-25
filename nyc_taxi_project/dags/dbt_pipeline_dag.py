"""
DAG principal du projet NYC Taxi Analytics.

Orchestre quotidiennement :
1. Ingestion des donnees meteo (API OpenWeather)
2. dbt seed (referentiels statiques : payment_type, vendor, rate_code)
3. dbt snapshot (historisation SCD2 des zones)
4. dbt run (staging puis marts, la couche intermediate a ete supprimee - cf. Fiches_Modelisation)
5. dbt test (45 tests)
6. dbt docs generate

Cible actuelle : dev (le passage a prod necessite d'avoir valide au prealable
la base NYC_TAXI_ANALYTICS_CI via GitHub Actions, non encore fait).
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append('/opt/airflow/scripts')

DBT_PROJECT_DIR = '/opt/airflow/dbt_project'
DBT_TARGET = 'dev'

default_args = {
    'owner': 'rooldy',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}


def run_weather_ingestion():
    """Wrapper pour appeler le script d'ingestion meteo depuis un PythonOperator."""
    import ingest_weather_data
    ingest_weather_data.main()


with DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    description='Pipeline complet NYC Taxi : ingestion meteo + dbt seed/snapshot/run/test/docs',
    schedule_interval='@daily',
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,  # Empeche deux runs de s'executer en parallele (evite les ecritures concurrentes en base)
    tags=['dbt', 'taxi', 'snowflake'],
) as dag:

    ingest_weather = PythonOperator(
        task_id='ingest_weather_data',
        python_callable=run_weather_ingestion,
    )

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt seed --target {DBT_TARGET}',
    )

    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt snapshot --target {DBT_TARGET}',
    )

    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select staging --target {DBT_TARGET}',
    )

    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select marts --target {DBT_TARGET}',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --target {DBT_TARGET}',
    )

    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt docs generate --target {DBT_TARGET}',
    )

    # Ordre des dependances : l'ingestion et les seeds peuvent tourner en parallele,
    # mais tout le reste doit suivre l'ordre staging -> marts -> test -> docs.
    # Le snapshot ne depend de rien d'autre que la source RAW_TAXI_ZONES (deja chargee).
    [ingest_weather, dbt_seed, dbt_snapshot] >> dbt_run_staging >> dbt_run_marts >> dbt_test >> dbt_docs